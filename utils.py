import math


def weighted_payouts(n_players, entry_fee, league_percentage):
    total_pot = n_players * entry_fee
    total_pot -= total_pot * league_percentage / 100
    n_winners = math.ceil(n_players / 3)

    # Basic descending weights for winners
    decay = 0.64
    weights = [decay ** i for i in range(n_winners)]
    total_weight = sum(weights)

    return [round((w / total_weight) * total_pot, 2) for w in weights]


def parse_league_urls(urls_text):
    """Parse league URLs stored as pipe-delimited text."""
    if not urls_text:
        return []
    return [url.strip() for url in urls_text.split('|') if url.strip()]


def format_league_urls(league_urls):
    """Format league URLs as a pipe-delimited string for storage."""
    if league_urls is None:
        return None
    if isinstance(league_urls, str):
        return league_urls.strip()
    return '|'.join(url.strip() for url in league_urls if url and url.strip())



def load_excel_file(file_path):
    """Load an Excel file and return its content as a list of dictionaries."""
    import pandas as pd
    try:
        df = pd.read_excel(file_path)
        return df.to_dict(orient='records')
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        return []