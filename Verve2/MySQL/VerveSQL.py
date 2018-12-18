import json
import webob
import pymysql.cursors
import time
import datetime
import configparser

#Verve Script
#Python version 3.4

#obtains verve server and SQL server values from config.ini file,
#establishes connection with verve server,
#obtains a JSON array with 8 sensor values,
#forwards the values to SQL server
#NOTE: table in SQL server MUST ALWAYS be named "verve"

config = configparser.ConfigParser()
config.read("config.ini")

class SQLInsertError(Exception):
    pass

class ConfigOptionsError(Exception):
    pass

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def config_section(section):
    optionsDict = {}
    options = config.options(section)
    for option in options:
        try:
            optionsDict[option] = config.get(section, option)
        except:
            raise ConfigOptionsError("Exception raised by " + option + " option.")
            optionsDict[option] = None
    return optionsDict

def retrieve_connection_info():
    try:
        #[General]
        cycles = config_section('General')['cycles'] #-1 = infinite
        minDelay = config_section('General')['min_delay'] #anything below 0.1s will default to 0.1s
        maxDelay = config_section('General')['max_delay']
        #[Verve]
        verveIP = config_section('Verve')['ip'] #obtain verve server values through config.ini file
        verveTimeout = config_section('Verve')['timeout'] #total number of seconds before code exits with timeout error
        verveRetries = config_section('Verve')['retries'] #number of retries within the timeout duration
        #[SQL]
        host = config_section('SQL')['host'] #obtain SQL server values through config.ini file
        user = config_section('SQL')['user']
        password = config_section('SQL')['password']
        db = config_section('SQL')['db']
        charset = config_section('SQL')['charset']
        SQLTimeout = config_section('SQL')['timeout']
        SQLRetries = config_section('SQL')['retries']
    except ConfigOptionsError as inst:
        raise ConfigOptionsError(inst)     
    except:
        raise ValueError("[General], [Verve] and/or [SQL] sections of config are formatted incorrectly.")
    return int(cycles), float(minDelay), float(maxDelay), verveIP, float(verveTimeout), int(verveRetries), host, user, password, db, charset, float(SQLTimeout), int(SQLRetries)
    
def insert_sensor_vals_into_sql_server(connection, vals, test):
    vals.append(test)
    try:
        with connection.cursor() as cursor: #insert data into SQL table
            sql = "INSERT INTO verve (sensor1, sensor2, sensor3, sensor4, sensor5, sensor6, sensor7, sensor8, testname) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, vals)
        connection.commit() #save changes to SQL table
    except:
            raise SQLInsertError("Insertion into SQL table unsuccessful.")
    finally:
        connection.close()

def sql_server_connect(host, user, password, db, charset, timeout, retries, maxEndTime):
    connection = ''
    startTime = datetime.datetime.now()
    endTime = startTime + datetime.timedelta(seconds = timeout)
    while connection == '' and datetime.datetime.now() < endTime: #keep retrying until time for timeout
        if datetime.datetime.now() >= maxEndTime:
            raise TimeoutError()
        else:
            try:
                connection = pymysql.connect(host = host, #connect to SQL server
                                             user = user,
                                             password = password,
                                             db = db,
                                             charset = charset,
                                             cursorclass = pymysql.cursors.DictCursor)
            except:
                continue
            finally:
                if connection == '':
                    time.sleep(int(timeout)/int(retries))
    if connection == '':
        raise ConnectionError("Connection to SQL server unsuccessful.")
    else:
        return connection

def verve_connect_and_parse(verveIP, timeout, retries, maxEndTime):
    resp = webob.Response()
    data = {}
    startTime = datetime.datetime.now()
    endTime = startTime + datetime.timedelta(seconds = timeout)
    while data == {} and datetime.datetime.now() < endTime: #keep retrying until time for timeout
        if datetime.datetime.now() >= maxEndTime:
            raise TimeoutError()
        else:
            try:
                req = webob.Request.blank(verveIP) #connect to verve server IP
                resp = req.get_response()
                data = json.loads(resp.body.decode()) #parse JSON that is received from verve
            except:
                continue
            finally:
                if data == {}:
                    time.sleep(int(timeout)/int(retries))
    if data == {}:
        raise ConnectionError("Connection to verve unsuccessful.")
    else:
        return data

def quadratic_fit(data):
    vals = []
    #[Sensor##] in config
    #format: ax^2 + bx + c, where x is the sensor's value and a, b, and c are constants from config.ini
    try:
        for n in range(1, 9):
            vals.append((int(config_section('Sensor0' + str(n))['a']) * (int(data["sensor0" + str(n)]) ** 2)) +
                        (int(config_section('Sensor0' + str(n))['b']) * int(data["sensor0" + str(n)])) +
                         int(config_section('Sensor0' + str(n))['c']))
    except:
        raise ValueError("[Sensor##] sections in config file are formatted incorrectly.")
    return vals

def main():
    cycles, minDelay, maxDelay, verveIP, verveTimeout, verveRetries, host, user, password, db, charset, SQLTimeout, SQLRetries = retrieve_connection_info()
    test = input("Input testname: ")
    currCycle = 0
    infiniteCycles = False
    if cycles == -1:
        infiniteCycles = True
    try:
        print("Initializing sensor data logging and transfer to SQL server...")
        print("Exit command: Ctrl + c")
        while infiniteCycles or (currCycle < cycles):
            startTime = datetime.datetime.now() #timing for min/max delay in config
            minEndTime = startTime + datetime.timedelta(seconds = minDelay)
            maxEndTime = startTime + datetime.timedelta(seconds = maxDelay)
            data = verve_connect_and_parse(verveIP, verveTimeout, verveRetries, maxEndTime)
            vals = quadratic_fit(data)
            print(vals)
            connection = sql_server_connect(host, user, password, db, charset, SQLTimeout, SQLRetries, maxEndTime)
            insert_sensor_vals_into_sql_server(connection, vals, test)
            currCycle += 1
            time.sleep(0.1) #at least 0.1 second intervals is a must between iterations; this is here for safety until microseconds can be integrated into SQL server datetime
            if datetime.datetime.now() < minEndTime:
                time.sleep((minEndTime - datetime.datetime.now()).total_seconds())
            elif datetime.datetime.now() > maxEndTime:
                raise TimeoutError()
    except TimeoutError:
        print("Maximum specified running time reached; process aborted.")
    except KeyboardInterrupt:
        print("Exit command received.")
    except Exception as e:
        print(e)

if __name__ == '__main__':
    main()
