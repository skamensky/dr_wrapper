import datetime
import os
import subprocess
from threading import Lock,Event,Thread
from multiprocessing import Process
import time
import re
import psutil
import traceback
from collections import namedtuple
from .constants import CONFIG

#set at the module level in order to override from another module if so desired
print = print

print_lock = Lock()

class LogWatcher(Thread):
    def __init__(self,organization_id=None,max_line_length=120,log_queue=None):
        super().__init__()
        self.organization_id=organization_id
        self._stop_event = Event()
        self.max_line_length=max_line_length
        self.log_queue=log_queue
    
    def stop(self):
        with print_lock:
            print('DemandTools LogWatcher stopped.')
        self._stop_event.set()

    def __exit__(self,type,value,traceback):
        self.stop()
        
    def __enter__(self):
        self.start()
        return self

    def _print(self,message):
        if self.log_queue:
            self.log_queue.put(message)
        print(message)

    @property
    def stopped(self):
        return self._stop_event.is_set()
        
    def run(self):
        base_log_directory = os.environ[CONFIG.LOGDIRECTORY_ENVIRONMENT_VARIABLE]
        if self.organization_id:
            log_directory = os.path.join(base_log_directory,self.organization_id)
        else:
            try:
                first_org_folder = [item for item in os.listdir(base_log_directory) if os.path.isdir(os.path.join(base_log_directory,item))][0]
                log_directory = os.path.join(base_log_directory,first_org_folder)
            except IndexError:
                self.stop()
                with print_lock:
                    raise Exception('No log folders found in {b}'.format(base_log_directory))
            
        expected_log_name='DemandToolsLog_{date}.txt'.format(date=datetime.date.today().strftime('%b%d%Y'))

        log_path = os.path.join(log_directory,expected_log_name)
        with print_lock:
            self._print('DemandTools LogWatcher initialized. Watching "{file_name}" for changes'.format(file_name=log_path))
        
        while not os.path.exists(log_path):
            if self.stopped:
                return
            time.sleep(1)                

        file = open(log_path)
        #got to end of file so as to only look for new lines
        file.seek(0,2)

        while True:
            if self.stopped:
                return
            where = file.tell()
            line = file.readline()
            if not line:
                time.sleep(1)
                file.seek(where)
            else:
                if line.replace("\n","")!="":
                    #first part of the line is a timestamp which we don't care about. Trim it off
                    if ',' in line:
                        line = line[line.index(',')+1:]
                    #remove commas and replace with pipes
                    line = re.sub(",+","|",line)
                    #remove new lines and quotes
                    line = re.sub('(\n|")+','',line)
                    line = line.strip()
                    with print_lock:
                        self._print('DTLog>>> {newline}'.format(newline=line[:self.max_line_length]))

def DemandToolsExceptionFactory(demand_tools_stderr_string):
    '''generates exception classes by parsing the standard out from demandtools exceptions'''
    
    class Sentence():
        def __init__(self,content,is_present=False):
            self.content=content
            self.is_present=is_present
        
    def sentences_in_demand_tools_stderr(sentences):
        for index,sentence in enumerate(sentences):
            if sentence.content in demand_tools_stderr_string:
                sentences[index].is_present=True
        return all([sentence.is_present for sentence in sentences])
    
    def is_db_access_collision_exception():
        return sentences_in_demand_tools_stderr([
                Sentence('because it is being used by another process'),
                Sentence('The process cannot access the file')
            ])
            
    def is_object_reference_exception():
        return sentences_in_demand_tools_stderr([Sentence('Object reference not set to an instance of an object')])
        
    if is_db_access_collision_exception():
        return DemandToolsMultiProcessDBWriteConflictException
    elif is_object_reference_exception():
        return DemandToolsObjectReferenceException
    else:
        return DemandToolsCommandException

class DemandToolsException(Exception):
    def __init__(self, message):
        super().__init__(message)
        
    def __repr__(self):
        return type(self).__name__

class DemandToolsMultiProcessDBWriteConflictException(DemandToolsException):
    def __init__(self, message):
        super().__init__(message)
        
class DemandToolsObjectReferenceException(DemandToolsException):
    def __init__(self, message):
        super().__init__(message)

class DemandToolsCommandException(DemandToolsException):
    def __init__(self, message):
        super().__init__(message)

class DemandToolsInputFileDoesNotExist(DemandToolsCommandException):
    def __init__(self, message):
        super().__init__(message)

class DTWrapperConfigException(Exception):
    def __init__(self, message):
        super().__init__(message)

class DemandToolsCommand(Process):
    
    _demand_tool_extension = {
    
        'dedupe' : '.STDxml',
        'mass_effect' : '.MExml',
        'mass_effect_export' : '.DExml',
        'bulk_backup' : '.BBxml'
    
    }
    
    print_prefix = 'DTCmd>>>'
    
    #the reverse of _demand_tool_extension
    _scenario_types = dict((v,k) for k,v in _demand_tool_extension.items())


    def __init__(self,
            scenario_path=None,
            input_file=None,
            output_file=None,
            extra_dt_args=None,
            post_run_func=None,
            post_run_func_args=(),
            post_run_func_kwargs={},
            debug=False,
            process_priority=psutil.IDLE_PRIORITY_CLASS,
            exceptions_to_retry_on=[],
            retry_count=0,
            log_queue=None
            ):
        self.scenario_path=scenario_path
        self.input_file=input_file
        self.output_file=output_file
        self.extra_dt_args=extra_dt_args
        self.post_run_func=post_run_func
        self.post_run_func_args = post_run_func_args
        self.post_run_func_kwargs = post_run_func_kwargs
        self.debug = debug
        self.process_priority = process_priority
        #if a different process is accessing DemandTool's local DB storage, DemandTools will throw an error.
        #this will allow you to catch that error and retry a number of times
        self.retry_count = retry_count
        self._retried_count = 0
        if not self.scenario_path:
            self.raise_exception(
                DemandToolsCommandException("scenario_path is a required argument")
            )
        self.exceptions_to_retry_on=exceptions_to_retry_on
        
        if not all([issubclass(e,Exception) for e in self.exceptions_to_retry_on]):
            self.raise_exception(
                DemandToolsCommandException('All exceptions_to_retry_on must inherit from Exception')
            )
        if bool(self.exceptions_to_retry_on) is not bool(self.retry_count):
            self.raise_exception(
                DemandToolsCommandException('If you set either retry_count or exceptions_to_retry_on you must set the other as well')
            )
        super().__init__(name=self.scenario_nice_name)
        self.log_queue=log_queue

    @property
    def _startupinfo(self):
        '''see https://stackoverflow.com/a/32121910/4188138 for more info and options'''
        #hide window
        SW_SETTING = 0
        if self.debug:
            #maximize window
            SW_SETTING = 3
        info = subprocess.STARTUPINFO()
        info.dwFlags = subprocess.STARTF_USESHOWWINDOW
        info.wShowWindow = SW_SETTING
        return info
        
    @property
    def demand_tools_args(self):
        args = ['demandtools',self.scenario_path]
        if self.input_file:
            args.append(self.input_file)
        if self.output_file:
            args.append(self.output_file)
        if self.extra_dt_args:
            args.append(self.extra_dt_args)
            
        return args
    

    @property
    def input_file_nice_name(self):
        if not self.input_file:
            return ''
        nice_name = os.path.basename(self.input_file)
        if '.' in nice_name:
            extension = nice_name[nice_name.rfind('.'):]
            nice_name = nice_name.replace(extension,'')
        return nice_name
        
    @classmethod
    def get_scenarios_in_path(cls,*,path):
        return [os.path.join(path,file) for file in os.listdir(path) if any([file.endswith(ext) for ext in cls._demand_tool_extension.values()])]
        
    @property
    def scenario_type(self):
        scenario_extension = self.scenario_path[self.scenario_path.rfind('.'):]
        try:
            return self._scenario_types[scenario_extension]
        except KeyError as e:
            self.raise_exception(
                DemandToolsCommandException('Unsupported extension "{e}" provided. Supported extensions are {se}'.format(
                    e=scenario_extension,
                    se=self._demand_tool_extension.values()
            )))

    @property
    def scenario_nice_name(self):
        extension = self._demand_tool_extension[self.scenario_type]
        return os.path.basename(self.scenario_path).replace(extension,"")

    @property
    def calculated_output_file_name(self):
        
        file_dir  = os.path.dirname(self.output_file)
        try:
            file_name = os.path.basename(os.path.splitext(self.output_file)[0])
        except IndexError:
            return ""
        #this is the date pattern that is generated by demandtools
        date_pattern = datetime.date.today().strftime('%Y%m%d')
        #start at 1 if it hasn't been run yet (since this is the predicted file name)
        times_ran_today = 1
        previous_runs = []
        for file in os.listdir(file_dir):
            pattern = r'{f}_{d}_([0-9])*\.csv'.format(f=file_name,d=date_pattern)
            match = re.match(pattern,file)
            if match and match.groups():
                previous_runs.append(int(match.groups()[0]))
        if previous_runs:
            times_ran_today = max(previous_runs)+1
        return '{output_file}_{date}_{times_ran}.csv'.format(
            output_file=os.path.join(file_dir,file_name),
            date=date_pattern,
            times_ran=times_ran_today
            )
            
    def _print(self,message):
        if self.log_queue:
            self.log_queue.put(message)
        with print_lock:
            print('{p}{m}'.format(
                p=DemandToolsCommand.print_prefix,
                m=message
            ))
            
    def raise_exception(self,exception_to_throw):
        #before we call super().__init__ the object has no name attribute
        name = '{process_name} Process'.format(process_name=self.name if hasattr(self,'name') else 'Unnamed')
        message = '{message}\nEnd Exception from {name}'.format(name=name,message=''.join([str(arg) for arg in exception_to_throw.args]))
        with print_lock:
            print('Start Exception from {name}'.format(name=name))
            raise type(exception_to_throw)(message) from None

    def run(self):
        '''
        !WARNING!
        Although DemandTools claims to officially support multiple instances of the process running simultaneously, I have personally encountered issues with multiprocessing
        https://skamensky.github.io/archives/validity/multiprocessing.html
        there are files that try to write to each other at the same time (temp database files) and DT simply crashing. When it crashes, there's nothing this package does to support
        recovery. However, if DT outputs standard output that indicates an error, you can use exceptions_to_retry_on to recuperate from those errors.
        '''
        psutil.Process(os.getpid()).nice(self.process_priority)
        
        if self.input_file:
            if not os.path.exists(self.input_file):
                self.raise_exception(DemandToolsInputFileDoesNotExist('{input_file} does not exist'.format(input_file=self.input_file)))
        self._print('Sending {type} scenario "{scenario}" to demandtools'.format(type=self.scenario_type, scenario=self.scenario_nice_name))

        if self.debug:
            self._print(self.demand_tools_args)
        with subprocess.Popen(self.demand_tools_args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,startupinfo=self._startupinfo) as proc:
            stdout,stderr = proc.communicate()
            if stderr:
                exception = DemandToolsExceptionFactory(stderr.decode())(stderr)
                if type(exception) in self.exceptions_to_retry_on and self.retry_count>self._retried_count:
                    self._retried_count+=1
                    self._print('Encountered {e} which was specified as a "retry error". Retrying (retry {n}/{d}) - {pname}'.format(
                        e=type(exception).__name__,
                        n=self._retried_count,
                        d=self.retry_count,
                        pname=self.scenario_nice_name
                    ))
                    self.run()
                    return
                else:
                    self.raise_exception(exception)
                
        self._print('demandtools is done processing {scenario}'.format(scenario=self.scenario_nice_name))
        if self.post_run_func:
            self.post_run_func(*self.post_run_func_args,**self.post_run_func_kwargs)
            
def preflight_checks():
    if CONFIG.LOGDIRECTORY_ENVIRONMENT_VARIABLE not in os.environ:
        raise DTWrapperConfigException("Missing demand tool log directory environment variable '{e}'"
            .format(e=CONFIG.LOGDIRECTORY_ENVIRONMENT_VARIABLE)
            )
            
preflight_checks()