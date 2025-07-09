import duckdb, requests


class TripDataTransformer:
    def __init__(self, year: int, output_path: str, base_url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data"):
        self.year = year
        self.output_path = output_path
        self.base_url = base_url
        self.input_uris = self._generate_valid_urls()

    def _generate_valid_urls(self) -> list:
        """Cek semua URL dan hanya simpan yang status 200."""
        urls = [
            f"{self.base_url}/yellow_tripdata_{self.year}-{month:02d}.parquet"
            for month in range(1, 13)
        ]
        valid_urls = []

        for url in urls:
            try:
                if requests.head(url, timeout=5).status_code == 200:
                    valid_urls.append(url)
            except:
                continue  # skip file yang tidak bisa diakses

        return valid_urls

    def _build_query(self) -> str:
        """Buat query SQL DuckDB untuk transformasi data."""
        return f"""
        COPY (
            SELECT
                strftime(tpep_pickup_datetime, '%Y-%m') AS month,
                COUNT(*) AS total_trips,
                ROUND(SUM(trip_distance), 2) AS total_distance,
                ROUND(AVG(trip_distance), 2) AS avg_distance,
                SUM(passenger_count)::INT AS total_passenger,
                printf('%.2f', ROUND(SUM(total_amount), 2)) AS total_revenue,
                ROUND(AVG(total_amount), 2) AS avg_revenue,
                ROUND(SUM(tip_amount), 2) AS total_tip,
                ROUND(AVG(tip_amount), 2) AS avg_tip,
                ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)), 4) AS avg_tip_percent
            FROM read_parquet(?)
            WHERE tpep_pickup_datetime BETWEEN '{self.year}-01-01' AND '{self.year}-12-31'
            GROUP BY month
            ORDER BY month
        )
        TO '{self.output_path}' (
            FORMAT 'parquet',
            PARTITION_BY 'month',
            OVERWRITE TRUE
        );
        """

    def run(self):
        """Jalankan proses transformasi."""
        if not self.input_uris:
            print(f"⚠️ No valid data files found for year {self.year}. Skipping transformation.")
            return

        query = self._build_query()

        try:
            duckdb.execute(query, [self.input_uris])
            print(f"✅ Data transformed for year {self.year} successfully.")
        except Exception as e:
            print(f"❌ Error during transformation for year {self.year}: {e}")


if __name__ == "__main__":
    transformer = TripDataTransformer(
        year=2025,
        output_path="/content/monthly_data"
    )
    transformer.run()
