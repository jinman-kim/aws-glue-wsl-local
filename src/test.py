import os
import time
import selenium

aws_profile = os.environ.get('AWS_PROFILE')
print(aws_profile)
for i in range(100):
    print(i)
    time.sleep(2)
