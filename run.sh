# Create tables: run create_dataset.sql
# Create example table: run create_sales_table.sql
# Create required tables: run create_score_table.sql or create_small_score_table.sql

# Run table: sales
python main.py

# Run table: paper_score / s_paper_score
# Schema = [paper_score, venue_name, venue_year, venue_type]
python main.py --table paper_score --k 10 --insight_dim 0 1 2

