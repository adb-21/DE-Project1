import random
import pandas as pd
import datetime
import time

def generate_sales(country_code, products, product_sale_frequencies, overall_sale_frequency,
                    selling_price_range, currencies):
    df = pd.DataFrame(columns=['product_code', 'country_of_sale', 'datetime', 'selling_price', 'currency'])
    for product_code in products:
    
        for i in range(int(product_sale_frequencies[product_code] * 100 * overall_sale_frequency * random.randint(1, 5))):
            datetime_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            selling_price = round(random.uniform(selling_price_range[product_code][0], selling_price_range[product_code][1]), 2)
            currency = currencies[country_code]
            
            df.loc[len(df)] = {
                'sale_id': hash(product_code + country_code + datetime_now + str(selling_price)),
                'product_code': product_code,
                'country_of_sale': country_code,
                'datetime': datetime_now,
                'selling_price': selling_price,
                'currency': currency
            }
            time.sleep(0.0001)  # Adjust the sleep time as needed to control the

    
    return df