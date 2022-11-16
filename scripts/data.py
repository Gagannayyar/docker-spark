import random
import string
import time


class CreateData:
    """
    creating a continious stream of data
    """

    def create_random_number():
        number = random.randint(1, 1000)
        return number

    def create_random_string():
        strings  = res = ''.join(random.choices(string.ascii_uppercase +
                             string.digits, k=random.randint(4, 12)))

        return strings


while True:
    value = CreateData.create_random_number()
    key = CreateData.create_random_string()
    dictionary = {
        key: value
    }
    print(dictionary)
    time.sleep(2)