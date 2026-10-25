from src.agents.llm_client import LLMClient
from src.agents.state import MappingItem, MappingProposal


ALIASES = {
    "orders": {
        "invoice_no": ["invoice_no", "InvoiceNo", "Invoice"],
        "stock_code": ["stock_code", "StockCode"],
        "description": ["description", "Description"],
        "quantity": ["quantity", "Quantity", "qty"],
        "invoice_date": ["invoice_date", "InvoiceDate"],
        "unit_price": ["unit_price", "UnitPrice", "price"],
        "customer_id": ["customer_id", "CustomerID"],
        "country": ["country", "Country"],
    },
    "customers": {
        "customer_id": ["customer_id"], "name": ["name"], "email": ["email"],
        "city": ["address.city", "city"], "country": ["address.country", "country"],
        "segment": ["segment"], "updated_at": ["updated_at"],
    },
    "returns": {
        "return_id": ["return_id"], "invoice_no": ["invoice_no"],
        "stock_code": ["stock_code"], "quantity": ["quantity", "qty_returned"],
        "return_date": ["return_date", "returned_on"], "reason": ["reason"],
    },
}


def propose_mapping(feed: str, source_cols: list[str], canonical_cols: list[str], llm: LLMClient | None = None) -> MappingProposal:
    items = []
    used = set()
    for canonical in canonical_cols:
        candidates = [name for name in ALIASES.get(feed, {}).get(canonical, [canonical]) if name in source_cols]
        if candidates:
            source = candidates[0]
            confidence = 1.0 if source == canonical else 0.95
            items.append(MappingItem(source_col=source, canonical_col=canonical, confidence=confidence, reason="known alias"))
            used.add(source)
    if llm and len(items) < len(canonical_cols):
        prompt = f"Feed: {feed}\nCanonical columns: {canonical_cols}\nSource columns: {source_cols}"
        proposal = llm.structured(
            "Return a MappingProposal JSON object. Only map source columns to canonical columns.",
            prompt,
            MappingProposal,
        )
        items.extend(proposal.mappings)
        used.update(item.source_col for item in proposal.mappings)
    return MappingProposal(
        feed=feed,
        mappings=items,
        unmapped_source_cols=[column for column in source_cols if column not in used],
    )
