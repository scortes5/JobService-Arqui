

def _parse_bedrooms(val) -> int | None:
    """
    Convierte "2 dormitorios" -> 2
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    # tomar el primer token como número
    try:
        return int(s.split()[0])
    except Exception:
        return None