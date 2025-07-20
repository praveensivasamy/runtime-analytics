def highlight_rank(val: int) -> str:
    """
    Highlights top ranks (e.g., rank = 1, 2, 3) with different background colors.
    """
    if val == 1:
        return "background-color: gold; font-weight: bold;"
    elif val == 2:
        return "background-color: silver; font-weight: bold;"
    elif val == 3:
        return "background-color: #cd7f32; font-weight: bold;"  # Bronze
    return ""
