import asyncio
import binascii
from bleak import BleakClient, BleakScanner
from bleak.backends.characteristic import BleakGATTCharacteristic
from influxdb import InfluxDBClient

actInfoResponse = bytearray(40)
rawdat={}

influxClient = InfluxDBClient(host='your_database_address_here.ddns.net', port=8086)

#after writing to ff02 data arrives by 2 packets and 2 notifications
def notification_handler(characteristic: BleakGATTCharacteristic, data: bytearray):
    print("Received: " + ''.join(format(x, '02x') for x in data))
    #clear buffer if received first half of packet
    if(data.startswith(b'\xdd\x03')):
        actInfoResponse.clear()
    #append to buffer if received second half
    for x in data:
        actInfoResponse.append(x)
    #print(actInfoResponse)
    #decode data to values if packet has valid start and end
    if (actInfoResponse.endswith(b'w')) and (actInfoResponse.startswith(b'\xdd\x03')):
        response=actInfoResponse[4:]

        rawdat['Vmain']=int.from_bytes(response[0:2], byteorder = 'big',signed=True)/100.0 #total voltage [V]
        rawdat['Imain']=int.from_bytes(response[2:4], byteorder = 'big',signed=True)/100.0 #current [A]
        rawdat['RemainAh']=int.from_bytes(response[4:6], byteorder = 'big',signed=True)/100.0 #remaining capacity [Ah]
        rawdat['NominalAh']=int.from_bytes(response[6:8], byteorder = 'big',signed=True)/100.0 #nominal capacity [Ah]
        rawdat['NumberCycles']=int.from_bytes(response[8:10], byteorder = 'big',signed=True) #number of cycles
        rawdat['ProtectState']=int.from_bytes(response[16:18],byteorder = 'big',signed=False) #protection state
        rawdat['ProtectStateBin']=format(rawdat['ProtectState'], '016b') #protection state binary
        rawdat['SoC']=int.from_bytes(response[19:20],byteorder = 'big',signed=False) #remaining capacity [%]
        rawdat['TempC1']=(int.from_bytes(response[23:25],byteorder = 'big',signed=True)-2731)/10.0
        rawdat['TempC2']=(int.from_bytes(response[25:27],byteorder = 'big',signed=True)-2731)/10.0
    
async def connect_to_battery(device: BleakClient):
    async with BleakClient(device) as client:
        print("Connected to " + device.address)
        #enable notifications when data in ff01 changed
        await client.start_notify("0000ff01-0000-1000-8000-00805f9b34fb", notification_handler)
        await asyncio.sleep(0.2)
        #for each write to ff02 value in ff01 is updated and notification handler processes data from ff01 characteristic 
        await client.write_gatt_char("0000ff02-0000-1000-8000-00805f9b34fb",bytes.fromhex('dd a5 03 00 ff fd 77'),False)
        await asyncio.sleep(1.0)
        await client.stop_notify("0000ff01-0000-1000-8000-00805f9b34fb")
        await client.disconnect()
    returndata=rawdat
    return returndata

async def main():
    
    print("starting scan...")

    device1 = await BleakScanner.find_device_by_name("***BLUETOOTH NAME OF BATTERY*****")
    device2 = await BleakScanner.find_device_by_name("***eg. 25.6V50Ah-148-2321*****")
    if None in (device1,device2):
        print("could not find device/devices")
        return

    battery1data = await connect_to_battery(device1)
    print("Battery 1: " + str(battery1data))
    battery2data = await connect_to_battery(device2)
    print("Battery 2: " + str(battery2data))
    if (battery1data=={} or battery2data=={}):
        print("No response from battery")
        return

    #if valid data read from battery make a string in influx line format and send it to database
    influxdata = []
    influxdata.append("bateria,akumulator=1 voltage=" + str(battery1data['Vmain']) +\
            ",current=" + str(battery1data['Imain']) +\
            ",actualCapacity=" + str(battery1data['RemainAh']) +\
            ",nominalCapacity=" + str(battery1data['NominalAh']) +\
            ",cycles=" + str(battery1data['NumberCycles']) +\
            ",protectState=" + str(battery1data['ProtectState']) +\
            ",soc=" + str(battery1data['SoC']) +\
            ",temperature=" + str(battery1data['TempC1'])+\
            "\n" +\
            "bateria,akumulator=2 voltage=" + str(battery2data['Vmain']) +\
            ",current=" + str(battery2data['Imain']) +\
            ",actualCapacity=" + str(battery2data['RemainAh']) +\
            ",nominalCapacity=" + str(battery2data['NominalAh']) +\
            ",cycles=" + str(battery2data['NumberCycles']) +\
            ",protectState=" + str(battery2data['ProtectState']) +\
            ",soc=" + str(battery2data['SoC']) +\
            ",temperature=" + str(battery2data['TempC1']))
    influxClient.write_points(influxdata, database='zberki_fotowoltaika', time_precision='ms', batch_size=10000, protocol='line')

asyncio.run(main())
