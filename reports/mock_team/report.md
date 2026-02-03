# Weekly Alternative Fuel Stations Snapshot

## Executive summary
- This report was generated automatically from the SQLite analytics database.
- Retry cap: 3. Deterministic generation: enabled.

## Stations by state

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

## Top cities by station count

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

## Restricted access footprint

**Question:** How many stations have restricted access?

**SQL:**
```sql
SELECT COUNT(*) FROM fuel_stations WHERE restricted_access = 1 LIMIT 200
```

### Table summary

- Returned 1 rows and 1 columns.
- Numeric column `COUNT(*)`: median 31, range 31 to 31.

**Preview:**

| COUNT(*) |
| --- |
| 31 |

## Fuel type mix

**Question:** Count stations by fuel_type_code

**SQL:**
```sql
SELECT fuel_type_code ,  COUNT(*) FROM fuel_stations GROUP BY fuel_type_code LIMIT 200
```

### Table summary

- Returned 1 rows and 2 columns.
- Numeric column `COUNT(*)`: median 500, range 500 to 500.
- Top `fuel_type_code` values: ELEC (1).

**Preview:**

| fuel_type_code | COUNT(*) |
| --- | --- |
| ELEC | 500 |

## California station sample

**Question:** Show 50 stations in California with station_name and street_address

**Status:** FAILED (oscillation)

**Last attempted SQL (best effort):**
```sql
:








SELECT station_name, street_address FROM fuel_stations WHERE state = 'CA' LIMIT 50
```
**Error:**
- Category: oscillation
- Message: Repeated SQL from attempt 1
