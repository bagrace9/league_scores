import math

def weighted_payouts(n_players, entry_fee, league_percentage):
    total_pot = n_players * entry_fee
    total_pot -= total_pot * league_percentage / 100
    n_winners = math.ceil(n_players / 3)

    # Basic descending weights for winners
    decay = 0.64
    weights = [decay ** i for i in range(n_winners)]
    total_weight = sum(weights)

    payouts = [round((w / total_weight) * total_pot, 2) for w in weights]
    return payouts


