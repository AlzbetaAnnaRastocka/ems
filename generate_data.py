import random

with open("random_numbers.txt", "w") as f:
    f.writelines(f"{random.randint(0, 1000000)}\n" for _ in range(100000000))