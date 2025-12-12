

class TradeFilter:

    @staticmethod
    def above_usd(trades: list[dict] | pd.DataFrame, threshold: float) -> list[dict]:
        if type(trades == list[dict]):
            return [trade for trade in trades if abs(float(trade["size"] * float(trade["price"]))) >= threshold]
        
        mask = abs(float(trades["price"]) * float(trades["size"])) >= threshold

        return trades[mask]

    @staticmethod
    # expects columns: market, volume_usd, month
    def market_monthly_volume(
            market_df: pd.DataFrame,
            threshold:float
    ) -> pd.DataFrame:
        monthly = (
            market_df.groupby(["market", market_df["date"].dt.to_period("M")])
            .volume_usd.sum()
            .reset_index()
        )

        high = monthly.groupby("market").volume_usd.sum()

        return market[market_df["market"].isin(high[high >= threshold].index)]


