from flask import Flask, render_template, request
import duckdb

app = Flask(__name__)
continuous_columns = ['humidity', 'temp', 'wind']
discrete_columns = ['day']
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
days = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]
sorted_months = sorted(months)

@app.route('/')
def index():
    scatter_ranges_query = f'SELECT MIN(X), MAX(X), MIN(Y), MAX(Y) FROM "forestfires.csv"' # Retrieves the minimum and maximum X and Y coordinates
    scatter_ranges_results = duckdb.sql(scatter_ranges_query).df()
    scatter_ranges = [
    float(scatter_ranges_results.iloc[0, 0]),
    float(scatter_ranges_results.iloc[0, 1]),
    float(scatter_ranges_results.iloc[0, 2]),
    float(scatter_ranges_results.iloc[0, 3]),
] # TODO: Define a list [minimum of X, maximum of X, minimum of Y, maximum of Y]
    
     # TODO: Write a query that retrieves the maximum number of forest fires that occurred in a single month
    max_count_query = '''SELECT MAX(month_count) AS max_count
        FROM (
            SELECT month, COUNT(*) AS month_count
            FROM "forestfires.csv"
            GROUP BY month
        )'''
    max_count_results = duckdb.sql(max_count_query).df()
    max_count = int(max_count_results.iloc[0, 0]) # TODO: Extract the maximum count from the query results
 
    # TODO: write a query that retrieves the the minimum and maximum value for each slider
    filter_ranges_query = '''
        SELECT
            MIN(humidity), MAX(humidity),
            MIN(temp), MAX(temp),
            MIN(wind), MAX(wind)
        FROM "forestfires.csv"
    '''
    filter_ranges_results = duckdb.sql(filter_ranges_query).df()
    # TODO: Create a dictionary where each key is a filter and values are the minimum and maximum values
    filter_ranges = {'humidity': [
            float(filter_ranges_results.iloc[0, 0]),
            float(filter_ranges_results.iloc[0, 1])
        ],
        'temp': [
            float(filter_ranges_results.iloc[0, 2]),
            float(filter_ranges_results.iloc[0, 3])
        ],
        'wind': [
            float(filter_ranges_results.iloc[0, 4]),
            float(filter_ranges_results.iloc[0, 5])
        ]} 

    return render_template(
        'index.html', months=months, days=days,
        filter_ranges=filter_ranges, scatter_ranges=scatter_ranges, max_count=max_count
    )

@app.route('/update', methods=["POST"])
def update():
    request_data = request.get_json()
    # TODO: update where clause from sliders
    continuous_predicate = ' AND '.join([f'({column} >= {request_data[column][0]} AND {column} <= {request_data[column][1]})' for column in continuous_columns]) 
    
    # TODO: update where clause from checkboxes
    selected_days = request_data['day']
    if len(selected_days) > 0:
        day_values = ', '.join([f"'{d}'" for d in selected_days])
        discrete_predicate = f"day IN ({day_values})"
    else:
        # If no day is selected, return no rows
        discrete_predicate = "1 = 0" 
    predicate = ' AND '.join([continuous_predicate, discrete_predicate]) # Combine where clause from sliders and checkboxes

    scatter_query = f'''
        SELECT X, Y
        FROM "forestfires.csv"
        WHERE {predicate}
    '''
    scatter_results = duckdb.sql(scatter_query).df()
    # TODO: Extract the data that will populate the scatter plot
    scatter_data = scatter_results.to_dict(orient='records') 
    

    bar_query =  f"""
        SELECT month, COUNT(*) AS count
        FROM "forestfires.csv"
        WHERE {predicate}
        GROUP BY month
        ORDER BY month
    """ # TODO: Write a query that retrieves the number of forest fires per month after filtering
    bar_results = duckdb.sql(bar_query).df()
    bar_results['month'] = bar_results.index.map({i: sorted_months[i] for i in range(len(sorted_months))})
    bar_data = bar_results[['month', 'count']].to_dict('records') # TODO: Extract the data that will populate the bar chart from the results
    max_count = int(bar_results['count'].max()) if len(bar_results) > 0 else 0 # TODO: Extract the maximum number of forest fires in a single month from the results

    return {'scatter_data': scatter_data, 'bar_data': bar_data, 'max_count': max_count}

if __name__ == "__main__":
    app.run(debug=True)
    