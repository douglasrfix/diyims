# Windows commands used or considered

How to execute a windows command from a python function.

[Schtasks](<https://learn.microsoft.com/en-us/windows/win32/taskschd/schtasks>).



import os
os.system('dir')


import os
output = os.popen('dir').readlines()
print(output)
