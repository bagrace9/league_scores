"""
League domain object for the league scores application.

Wraps the database layer to provide a clean interface for creating,
reading, and updating league records and their handicap settings.
"""
from database import create_league as db_create_league
from database import update_league as db_update_league
from database import fetch_leagues as db_fetch_leagues
from database import fetch_league_by_id as db_fetch_league_by_id


class League:
    """Class representing a disc golf league with all its properties and operations."""

    def __init__(
        self,
        league_id=None,
        name="",
        urls=None,
        cash_percentage=0.0,
        entry_fee=0.0,
        is_handicap=False,
        handicap_minimum_rounds=0,
        handicap_rounds_considered=0,
        handicap_years_lookback=0,
        handicap_base_score=0,
        handicap_multiplier=0.0,
    ):
        self.id = league_id
        self.name = name
        self.urls = urls if urls is not None else []
        self.cash_percentage = cash_percentage
        self.entry_fee = entry_fee
        self.is_handicap = is_handicap
        self.handicap_minimum_rounds = handicap_minimum_rounds
        self.handicap_rounds_considered = handicap_rounds_considered
        self.handicap_years_lookback = handicap_years_lookback
        self.handicap_base_score = handicap_base_score
        self.handicap_multiplier = handicap_multiplier

    @classmethod
    def create(
        cls,
        name,
        urls,
        cash_percentage,
        entry_fee,
        is_handicap,
        handicap_minimum_rounds,
        handicap_rounds_considered,
        handicap_years_lookback,
        handicap_base_score,
        handicap_multiplier,
    ):
        """Create a new league in the database."""
        league_id = db_create_league(
            name,
            urls,
            cash_percentage,
            entry_fee,
            is_handicap,
            handicap_minimum_rounds,
            handicap_rounds_considered,
            handicap_years_lookback,
            handicap_base_score,
            handicap_multiplier,
        )

        return cls(
            league_id=league_id,
            name=name,
            urls=urls,
            cash_percentage=cash_percentage,
            entry_fee=entry_fee,
            is_handicap=is_handicap,
            handicap_minimum_rounds=handicap_minimum_rounds,
            handicap_rounds_considered=handicap_rounds_considered,
            handicap_years_lookback=handicap_years_lookback,
            handicap_base_score=handicap_base_score,
            handicap_multiplier=handicap_multiplier,
        )

    def update(self):
        """Update this league in the database."""
        if self.id is None:
            raise ValueError("Cannot update league without ID")

        db_update_league(
            self.id,
            self.name,
            self.urls,
            self.cash_percentage,
            self.entry_fee,
            self.is_handicap,
            self.handicap_minimum_rounds,
            self.handicap_rounds_considered,
            self.handicap_years_lookback,
            self.handicap_base_score,
            self.handicap_multiplier,
        )

    @classmethod
    def get_all(cls):
        """Get all leagues from the database."""
        leagues_data = db_fetch_leagues()
        leagues = []
        for league_id, _ in leagues_data:
            league = cls.get_by_id(league_id)
            if league is not None:
                leagues.append(league)
        return leagues

    @classmethod
    def get_by_id(cls, league_id):
        """Get a league by its ID."""
        league_data = db_fetch_league_by_id(league_id)
        if league_data is None:
            return None

        return cls(
            league_id=league_id,
            name=league_data['name'],
            urls=league_data['urls'],
            cash_percentage=league_data['cash_percentage'],
            entry_fee=league_data['entry_fee'],
            is_handicap=league_data['is_handicap'],
            handicap_minimum_rounds=league_data['handicap_minimum_rounds'],
            handicap_rounds_considered=league_data['handicap_rounds_considered'],
            handicap_years_lookback=league_data['handicap_years_lookback'],
            handicap_base_score=league_data['handicap_base_score'],
            handicap_multiplier=league_data['handicap_multiplier'],
        )

    def add_url(self, url):
        """Add a URL to this league."""
        if url not in self.urls:
            self.urls.append(url)

    def remove_url(self, url):
        """Remove a URL from this league."""
        if url in self.urls:
            self.urls.remove(url)

    def get_handicap_settings(self):
        """Get handicap settings as a dictionary."""
        if not self.is_handicap:
            return None

        return {
            'minimum_rounds': self.handicap_minimum_rounds,
            'rounds_considered': self.handicap_rounds_considered,
            'years_lookback': self.handicap_years_lookback,
            'base_score': self.handicap_base_score,
            'multiplier': self.handicap_multiplier,
        }

    def __str__(self):
        return f"League(id={self.id}, name='{self.name}', urls={len(self.urls)} URLs)"

    def __repr__(self):
        return self.__str__()
