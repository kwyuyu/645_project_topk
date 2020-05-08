# Create required tables:
# Run q1.sql, q2.sql, q3.sql, q4.sql, and extension.sql in postgres

# Command:
# python main.py \
#    --table [table_name] \
#    --k [top_k (default=10)] \
#    --insight_dim [list_of_dimension (the first element should be the measure.)]

# Q1. tau = 2
python main.py --table q1 --k 10 --insight_dim 2 0
python main.py --table q1 --k 10 --insight_dim 2 1
# Q1. tau = 3
python main.py --table q1 --k 10 --insight_dim 2 0 1

# Q2.
python main.py --table q2 --k 10 --insight_dim 2 0
python main.py --table q2 --k 10 --insight_dim 2 1

# Q3.
python main.py --table q3 --k 10 --insight_dim 2 0
python main.py --table q3 --k 10 --insight_dim 2 1

# Q4.
python main.py --table q4 --k 10 --insight_dim 2 0
python main.py --table q4 --k 10 --insight_dim 2 1

# Extension.
python main.py --table ext_dl --k 10 --insight_dim 2 0
python main.py --table ext_dl --k 10 --insight_dim 2 1

