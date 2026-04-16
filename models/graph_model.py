"""Graph construction utilities for transaction relationship topology."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import networkx as nx
import pandas as pd


@dataclass
class GraphBatchStats:
    """Incremental graph update statistics for one micro-batch."""

    batch_id: int
    processed_rows: int
    skipped_rows: int
    new_nodes: int
    new_edges: int
    total_nodes: int
    total_edges: int


class TransactionGraphBuilder:
    """Maintain an in-memory transaction graph with incremental batch updates."""

    def __init__(self) -> None:
        self.graph = nx.MultiDiGraph()
        self._auto_edge_counter = 0
        self._seen_transaction_ids: set[str] = set()
        self.latest_pagerank_scores: dict[str, float] = {}
        self.latest_community_assignments: dict[str, int] = {}
        self.latest_triangle_counts: dict[str, int] = {}
        self.latest_component_assignments: dict[str, int] = {}
        self.latest_component_sizes: dict[int, int] = {}
        self.latest_propagated_risk_scores: dict[str, float] = {}

    def update_from_pandas(self, frame: pd.DataFrame, batch_id: int) -> GraphBatchStats:
        """Update graph state from a scored micro-batch represented as pandas rows."""
        new_nodes = 0
        new_edges = 0
        skipped_rows = 0

        for row in frame.to_dict(orient="records"):
            customer_node = self._resolve_customer_node(row)
            merchant_node = self._resolve_merchant_node(row)

            if customer_node is None or merchant_node is None:
                skipped_rows += 1
                continue

            if customer_node not in self.graph:
                self.graph.add_node(
                    customer_node,
                    node_type="customer",
                    entity_id=self._resolve_customer_identifier(row),
                )
                new_nodes += 1

            if merchant_node not in self.graph:
                self.graph.add_node(
                    merchant_node,
                    node_type="merchant",
                    entity_id=self._get_normalized_value(row.get("merchant_id")),
                )
                new_nodes += 1

            transaction_id = self._get_normalized_value(row.get("transaction_id"))
            if transaction_id and transaction_id in self._seen_transaction_ids:
                continue

            edge_key = transaction_id or self._next_auto_edge_key()
            if transaction_id:
                self._seen_transaction_ids.add(transaction_id)

            self.graph.add_edge(
                customer_node,
                merchant_node,
                key=edge_key,
                transaction_id=transaction_id,
                amount=self._safe_float(row.get("amount")),
                timestamp=self._get_normalized_value(row.get("timestamp")),
                fraud_probability=self._safe_float(row.get("fraud_probability")),
                anomaly_score=self._safe_float(row.get("anomaly_score")),
            )
            new_edges += 1

        return GraphBatchStats(
            batch_id=batch_id,
            processed_rows=len(frame.index),
            skipped_rows=skipped_rows,
            new_nodes=new_nodes,
            new_edges=new_edges,
            total_nodes=self.graph.number_of_nodes(),
            total_edges=self.graph.number_of_edges(),
        )

    def compute_pagerank(self) -> dict[str, float]:
        """Compute PageRank on the current transaction graph and store latest scores."""
        if self.graph.number_of_nodes() == 0:
            self.latest_pagerank_scores = {}
            return self.latest_pagerank_scores

        pagerank_scores = nx.pagerank(self.graph)
        self.latest_pagerank_scores = {node_id: float(score) for node_id, score in pagerank_scores.items()}
        return self.latest_pagerank_scores

    def compute_communities(self) -> dict[str, int]:
        """Compute Louvain communities on current graph and store latest assignments."""
        if self.graph.number_of_nodes() == 0:
            self.latest_community_assignments = {}
            return self.latest_community_assignments

        undirected_graph = nx.Graph()
        for source, target, data in self.graph.edges(data=True):
            weight = self._safe_float(data.get("amount")) or 1.0
            if undirected_graph.has_edge(source, target):
                undirected_graph[source][target]["weight"] += weight
            else:
                undirected_graph.add_edge(source, target, weight=weight)

        if undirected_graph.number_of_edges() == 0:
            self.latest_community_assignments = {
                node_id: index for index, node_id in enumerate(sorted(undirected_graph.nodes()))
            }
            return self.latest_community_assignments

        try:
            communities = nx.community.louvain_communities(
                undirected_graph,
                weight="weight",
                seed=42,
            )
        except AttributeError as exc:
            raise RuntimeError(
                "Louvain community detection is unavailable in current NetworkX version. "
                "Upgrade NetworkX to a version that supports community.louvain_communities."
            ) from exc

        assignments: dict[str, int] = {}
        for community_id, community_nodes in enumerate(communities):
            for node_id in community_nodes:
                assignments[str(node_id)] = community_id

        self.latest_community_assignments = assignments
        return self.latest_community_assignments

    def compute_triangle_counts(self) -> dict[str, int]:
        """Compute triangle counts for all nodes and store latest counts."""
        if self.graph.number_of_nodes() == 0:
            self.latest_triangle_counts = {}
            return self.latest_triangle_counts

        undirected_graph = nx.Graph()
        undirected_graph.add_nodes_from(self.graph.nodes())
        undirected_graph.add_edges_from(self.graph.edges())

        triangle_counts = nx.triangles(undirected_graph)
        self.latest_triangle_counts = {
            str(node_id): int(count)
            for node_id, count in triangle_counts.items()
        }
        return self.latest_triangle_counts

    def compute_connected_components(self) -> tuple[dict[str, int], dict[int, int]]:
        """Compute connected components and store latest node/component mappings."""
        if self.graph.number_of_nodes() == 0:
            self.latest_component_assignments = {}
            self.latest_component_sizes = {}
            return self.latest_component_assignments, self.latest_component_sizes

        undirected_graph = nx.Graph()
        undirected_graph.add_nodes_from(self.graph.nodes())
        undirected_graph.add_edges_from(self.graph.edges())

        component_assignments: dict[str, int] = {}
        component_sizes: dict[int, int] = {}
        for component_id, component_nodes in enumerate(nx.connected_components(undirected_graph)):
            size = int(len(component_nodes))
            component_sizes[component_id] = size
            for node_id in component_nodes:
                component_assignments[str(node_id)] = component_id

        self.latest_component_assignments = component_assignments
        self.latest_component_sizes = component_sizes
        return self.latest_component_assignments, self.latest_component_sizes

    def compute_risk_propagation(self) -> dict[str, float]:
        """Compute deterministic propagated risk from node signals and neighboring exposure."""
        if self.graph.number_of_nodes() == 0:
            self.latest_propagated_risk_scores = {}
            return self.latest_propagated_risk_scores

        component_size_by_node = {
            node_id: int(self.latest_component_sizes.get(component_id, 0))
            for node_id, component_id in self.latest_component_assignments.items()
        }

        node_risk_seed: dict[str, float] = {}
        for node_id in self.graph.nodes():
            node_id_str = str(node_id)
            edge_exposure_risk = self._compute_node_edge_risk(node_id_str)
            pagerank_signal = float(self.latest_pagerank_scores.get(node_id_str, 0.0))
            triangle_signal = float(self.latest_triangle_counts.get(node_id_str, 0))
            component_signal = float(component_size_by_node.get(node_id_str, 0))

            normalized_triangle = triangle_signal / (1.0 + triangle_signal)
            normalized_component = component_signal / (1.0 + component_signal)
            normalized_pagerank = pagerank_signal / (1.0 + pagerank_signal)

            base_risk = (
                0.55 * edge_exposure_risk
                + 0.20 * normalized_pagerank
                + 0.15 * normalized_triangle
                + 0.10 * normalized_component
            )
            node_risk_seed[node_id_str] = self._clip01(base_risk)

        propagated: dict[str, float] = {}
        for node_id in self.graph.nodes():
            node_id_str = str(node_id)
            neighbors = set(str(neighbor) for neighbor in self.graph.predecessors(node_id))
            neighbors.update(str(neighbor) for neighbor in self.graph.successors(node_id))

            if not neighbors:
                propagated[node_id_str] = node_risk_seed[node_id_str]
                continue

            neighbor_avg = sum(node_risk_seed.get(neighbor_id, 0.0) for neighbor_id in neighbors) / len(neighbors)
            propagated_score = 0.7 * node_risk_seed[node_id_str] + 0.3 * neighbor_avg
            propagated[node_id_str] = self._clip01(propagated_score)

        self.latest_propagated_risk_scores = propagated
        return self.latest_propagated_risk_scores

    def _compute_node_edge_risk(self, node_id: str) -> float:
        """Aggregate direct transaction risk from incident edges around a node."""
        exposures: list[float] = []

        for _, _, edge_data in self.graph.in_edges(node_id, data=True):
            exposures.append(self._edge_risk(edge_data))
        for _, _, edge_data in self.graph.out_edges(node_id, data=True):
            exposures.append(self._edge_risk(edge_data))

        if not exposures:
            return 0.0
        return self._clip01(sum(exposures) / len(exposures))

    def _edge_risk(self, edge_data: dict[str, Any]) -> float:
        """Convert edge-level fraud and anomaly attributes into a bounded risk score."""
        fraud_probability = self._safe_float(edge_data.get("fraud_probability"))
        anomaly_score = self._safe_float(edge_data.get("anomaly_score"))

        fraud_signal = self._clip01(fraud_probability if fraud_probability is not None else 0.0)
        anomaly_signal_raw = anomaly_score if anomaly_score is not None else 0.0
        anomaly_signal = self._clip01(anomaly_signal_raw / (1.0 + abs(anomaly_signal_raw)))

        return self._clip01(0.7 * fraud_signal + 0.3 * anomaly_signal)

    def _resolve_customer_node(self, row: dict[str, Any]) -> str | None:
        """Resolve customer node key using user_id with account_id fallback."""
        identifier = self._resolve_customer_identifier(row)
        if identifier is None:
            return None
        return f"customer:{identifier}"

    def _resolve_customer_identifier(self, row: dict[str, Any]) -> str | None:
        """Resolve customer identifier from row payload."""
        user_id = self._get_normalized_value(row.get("user_id"))
        if user_id is not None:
            return user_id
        return self._get_normalized_value(row.get("account_id"))

    def _resolve_merchant_node(self, row: dict[str, Any]) -> str | None:
        """Resolve merchant node key from row payload."""
        merchant_id = self._get_normalized_value(row.get("merchant_id"))
        if merchant_id is None:
            return None
        return f"merchant:{merchant_id}"

    def _next_auto_edge_key(self) -> str:
        """Generate unique fallback edge key when transaction_id is unavailable."""
        self._auto_edge_counter += 1
        return f"auto-edge-{self._auto_edge_counter}"

    @staticmethod
    def _safe_float(value: Any) -> float | None:
        """Safely convert scalar values to float for numeric edge attributes."""
        if value is None:
            return None
        try:
            converted = float(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(converted):
            return None
        return converted

    @staticmethod
    def _get_normalized_value(value: Any) -> str | None:
        """Normalize identifiers and text fields to clean strings."""
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None

        normalized = str(value).strip()
        if not normalized:
            return None
        return normalized

    @staticmethod
    def _clip01(value: float) -> float:
        """Clamp numeric values into closed interval [0, 1]."""
        return max(0.0, min(1.0, float(value)))


class GraphRiskScorer:
    """Compatibility wrapper exposing graph updates while scoring is out of scope."""

    def __init__(self) -> None:
        self._builder = TransactionGraphBuilder()

    def update_graph(self, event: dict[str, Any]) -> None:
        """Update relationship graph with a single transaction event."""
        frame = pd.DataFrame([event])
        self._builder.update_from_pandas(frame=frame, batch_id=0)

    def score_entity(self, entity_id: str) -> float:
        """Return placeholder score until graph-risk algorithms are introduced."""
        _ = entity_id
        raise NotImplementedError("Graph scoring is not implemented yet. Only graph construction is supported.")
