# Mock Analytics Team Report

## Executive summary
- Database: `tests/fixtures/nrel_sample.sqlite`
- Retry cap: 3
- Successful queries: 2/2
- Read-only execution: enabled

## Query 1

**Question:** How many stations are there by state?

**SQL:**
```sql
SELECT state, COUNT(*) FROM fuel_stations GROUP BY state LIMIT 200
```

### Table summary

- Returned 1 rows and 2 columns.
- Numeric column `COUNT(*)`: median 500, range 500 to 500.
- Top `state` values: WA (1).

**Preview:**

| state | COUNT(*) |
| --- | --- |
| WA | 500 |

## Query 2

**Question:** Top 10 cities by station count

**SQL:**
```sql
SELECT city, COUNT(*) AS station_count FROM fuel_stations GROUP BY city ORDER BY station_count DESC LIMIT 10
```

### Table summary

- Returned 10 rows and 2 columns.
- Numeric column `station_count`: median 16, range 10 to 84.
- Top `city` values: Seattle (1), Bellevue (1), Tacoma (1), Renton (1), Kirkland (1).

**Preview:**

| city | station_count |
| --- | --- |
| Seattle | 84 |
| Bellevue | 43 |
| Tacoma | 23 |
| Renton | 20 |
| Kirkland | 17 |
| Bremerton | 16 |
| Spokane | 15 |
| Issaquah | 11 |
| Bellingham | 11 |
| Vancouver | 10 |
