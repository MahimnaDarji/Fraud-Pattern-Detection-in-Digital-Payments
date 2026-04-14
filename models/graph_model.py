"""Graph-based fraud risk model scaffold using NetworkX-style relationships."""

from typing import Any


class GraphRiskScorer:
    """Placeholder for account-device-merchant graph risk scoring."""

    def update_graph(self, event: dict[str, Any]) -> None:
        """Update relationship graph with a new transaction event."""
        raise NotImplementedError("Graph update logic is not implemented yet.")

    def score_entity(self, entity_id: str) -> float:
        """Return a risk score for a graph entity."""
        raise NotImplementedError("Graph scoring logic is not implemented yet.")
