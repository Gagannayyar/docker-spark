import random
import string
import time
import names
import pandas as pd

class CreateData:

    "Create a small stream of dataframes for practice"

    def __init__(self):
        pass


    def create_random_number(self): 
        number = random.randint(1, 1000)
        return number

    def create_random_string(self):
        strings  = res = ''.join(random.choices(string.ascii_uppercase +string.digits, k=random.randint(3,10)))

        return strings
            
    def create_names(self):
        name = names.get_full_name()
        return str(name)
    
    def create_data_dict(self,num):
        """
        Create a dictionary to be converted to dataframe in next step
        Args:
            num: The number of key, value pairs to be created
        """
        names = []
        string = []
        number = []
        for item in range(0,num):
            names.append(self.create_names())
            string.append(self.create_random_string())
            number.append(self.create_random_number())

            dicti = {
                    "name": names,
                    "code": string,
                    "number": number
                }
        return dicti
    
    def create_dataframe(self,num):
        dicti = self.create_data_dict(num)
        df = pd.DataFrame(data=dicti)
        return df
    
    def df_pertime(self,number_of_df,num,sleep_time=10):
        """
        Keep create a batch of dataframe with specific number of rows
        args:
            number_of_df: Number of dataframes to be created
            num: Number of rows in dataframe
            sleep_time: The time between consective dataframes in seconds 
            (default is 10 seconds)
        """
        n = 1
        while n <= number_of_df  :
            df = self.create_dataframe(num)
            n += 1
            time.sleep(sleep_time)
            return df
