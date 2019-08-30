from .demandtools_wrapper import LogWatcher,DemandToolsCommand,DemandToolsCommandException,DemandToolsInputFileDoesNotExist
import os

demand_tools_exec_found = False

for path in os.environ['PATH'].split(';'):    
    try:
        for file_p in os.listdir(path):
            if file_p.lower()=='demandtools.exe':
                demand_tools_exec_found = True
                break
    except OSError as e:
        pass

if demand_tools_exec_found is not True:
    raise Exception('DemandTools executable not found in path. Add the directory that contains the demand tools executable to the path environment variable')

#keep this on its own line, setup.py depends on it
__version__ = "0.47"
    
#cleanup local variables
del demand_tools_exec_found
del file_p
del path
del os