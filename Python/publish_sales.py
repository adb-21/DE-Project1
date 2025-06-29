import multiprocessing
import time
import random
import pandas as pd
from get_country_sales import send_sales 

if __name__ == "__main__":
    countries = ['US', 'UK', 'DE', 'FR', 'IT']

    products = ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7', 'p8', 'p9', 'p10']
    product_sale_frequency = {'p1': 0.2, 'p2': 0.15, 'p3': 0.1, 'p4': 0.25, 'p5': 0.05, 'p6': 0.1, 'p7': 0.05, 'p8': 0.03, 'p9': 0.02, 'p10': 0.01}
    overall_sale_frequency = 2
    selling_price_range = {'p1': (10, 11), 'p2': (20, 22), 'p3': (30, 30.5), 'p4': (40, 41), 'p5': (50, 50.1),
                            'p6': (60, 60.05), 'p7': (70, 75), 'p8': (80, 82), 'p9': (90.1, 90.2), 'p10': (100, 100.5)}
    # Define the currency mapping for each country
    currencies = {'US': 'USD', 'UK': 'GBP', 'DE': 'EUR', 'FR': 'EUR', 'IT': 'EUR'}

    delay = (10, 15)
    max_iterations = 5     #This will be removed when continuous streaming is enabled

    processes = []
    # Loop to generate sales data for a specific country
    for country in countries:
        # Create a process for each country
        process = multiprocessing.Process(target=send_sales, args=(country, products, product_sale_frequency, overall_sale_frequency, 
                                                                selling_price_range, currencies, delay, max_iterations))
        processes.append(process)
        process.start()  # Start the process
        print(f"Process {country} started")


    for process in processes:
        process.join()
