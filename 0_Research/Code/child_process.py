# # Name:                                            Renacin Matadeen
# # Date:                                               01/10/2024
# # Title                                 Do Child Threads Still Hold Onto Memory?
# #
# # ----------------------------------------------------------------------------------------------------------------------
from time import sleep
from concurrent.futures import ProcessPoolExecutor
# # ----------------------------------------------------------------------------------------------------------------------

def task():
    sleep(1)

if __name__ == '__main__':
    with ProcessPoolExecutor() as exe:
        exe.submit(task)
