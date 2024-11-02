# Name:                                            Renacin Matadeen
# Date:                                               01/10/2024
# Title                                 Do Child Threads Still Hold Onto Memory?
#
# ----------------------------------------------------------------------------------------------------------------------
import time
from concurrent.futures import ThreadPoolExecutor
# ----------------------------------------------------------------------------------------------------------------------

# for var, obj in list(locals().items()):
#     print(var, sys.getsizeof(obj))

# Define Function
def test_func():
	test_list = [x for x in range(100_000_000)]


with ThreadPoolExecutor(max_workers=1) as executor:
	result = executor.submit(test_func).result()
	# executor.submit(test_func)
