import re

def _parse_bedrooms(val) -> int | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None

    match = re.search(r"\d+", s)  # busca el primer grupo de dígitos
    if not match:
        return None
    try:
        return int(match.group(0))
    except Exception:
        return None
