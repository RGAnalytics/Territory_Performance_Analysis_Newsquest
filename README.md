# Territory Performance Analysis (SQL and Power BI)

Ranking sales territories by how efficiently they have been farmed, and finding where the biggest growth potential sits, for a regional media advertising company.

> Analysis of customer sales data from Newsquest, a regional media advertising company. The data cleaning, SQL, MCDA ranking, Power BI dashboard and recommendations are my own work.

**Tools:** MySQL, Power BI, Excel &nbsp;|&nbsp; **Techniques:** data cleaning, CTEs, window functions (`ROW_NUMBER`), multi-criteria decision analysis (MCDA)

## Business questions

1. **Which territories have been farmed most efficiently, and how should they be ranked?**
2. **Which one or two territories have the greatest future potential?**
3. **How could the top-potential territory be optimised, and what extra data would help?**

## Data

Customer sales records, where each row is a customer interaction. Each customer belongs to one of 11 sales territories, with one sales rep per territory.

| Field group | Fields |
|---|---|
| Customer details | Name, business type, estimated turnover |
| Location | Town, postcode sector, territory |
| Activity | Active customer (has advertised before), last contact date, last booking date |
| Performance | Revenue over the last 5 years and the last year |

The raw data is not included in this repo.

## Approach

**1. Cleaning in SQL** ([Territory_Performance.sql](1_sql/Territory_Performance.sql))
- Copied the data into a staging table so the raw table stayed untouched
- Found duplicates using `ROW_NUMBER()` over all columns in a CTE
- Standardised town names (for example "Gt Dunmow" became "Great Dunmow")
- Removed inconsistent records (inactive customers showing past revenue)

**2. Farming KPIs in SQL**
- Percentage of repeated customers per territory, using CTEs and a `LEFT JOIN`
- Total revenue for the last 5 years and the last year
- Bookings made by repeated customers
- Average days since last contact

**3. MCDA ranking**
Each territory was scored from 1 to 11 on four weighted criteria, then ranked on the total:

| Criterion | Weight |
|---|---|
| Repeated customers (%) | 35% |
| Bookings by repeated customers | 30% |
| Revenue, last 5 years | 25% |
| Revenue, last year | 10% |

**4. Power BI dashboard**
KPIs, revenue by territory and business type, customer segments, and active vs inactive customers by territory.

## Key insights

- **11,480 customers across 11 territories**, generating £40M over 5 years and £5M in the last year.
- **Retail drives the business:** 61.6% of 5-year revenue.
- **Territory 3 is the most efficiently farmed** (MCDA score 10.35 out of 11), followed by Territory 5 (9.65) and Territory 4 (9.1). It also has the highest 5-year revenue at £9.3M.
- **Territory 2 has the greatest future potential.** It has the most inactive customers (1,291) and a large share of businesses with £100k to £750k turnover. It ranked first in a second MCDA on potential (2.4, ahead of Territory 3 at 2.0).
- **Recommendations for Territory 2:** improve data completeness, run targeted campaigns and tailored incentives for dormant customers, increase sales rep contact, and bring in reps from high-performing territories to share what works.

## Output

### Power BI dashboard
<img width="1141" height="586" alt="image" src="https://github.com/user-attachments/assets/05371840-c062-4fdf-9e3e-d6e55a57b3f7" />
<img width="1075" height="570" alt="image" src="https://github.com/user-attachments/assets/35ad38fd-521d-46a9-9dc5-9ee2f0c8a7cc" />



### MCDA territory ranking
<img width="1041" height="378" alt="image" src="https://github.com/user-attachments/assets/c909dc45-23f7-455b-8258-65b5344a6ee6" />


### Territory potential
<img width="1047" height="518" alt="image" src="https://github.com/user-attachments/assets/ccaeb0dd-f886-4326-a3a2-1b707fe7ba89" />


## Files

- **SQL workings:** [Territory_Performance.sql](1_sql/Territory_Performance.sql)
- **Dashboard:** [Sales_Dashboard.pdf](2_powerbi/Sales_Dashboard.pdf)
- **Presentation:** [Territory_Performance.pdf](3_presentation/Territory_Performance%20.pdf)

## What I'd improve next

- Fix the duplicate removal step: the current `DELETE` removes every copy of a duplicated row. I'd delete only rows where `row_num > 1`.
- Add contact recency to the MCDA, since I calculated it in SQL but didn't use it in the ranking.
- Build a revenue forecast for Territory 2, which would need monthly revenue per customer, sales rep activity logs and economic data.
