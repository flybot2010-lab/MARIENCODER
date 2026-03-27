import paramiko
import os
import yaml
import argparse
import ntplib
from datetime import datetime, timezone
import logging
import coloredlogs

config = yaml.safe_load(open("config.yaml"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
coloredlogs_config = config['SYSTEM']['COLOREDLOGS'].copy()
coloredlogs_config['logger'] = logger
coloredlogs.install(**coloredlogs_config)

STUPID_FUCKING_TWC_CORBA_ERROR = "twccommon.corba.CosEventChannelAdmin._objref_ProxyPushConsumer instance"

data_root = os.path.join(os.path.dirname(__file__), "output")

def enumerate_the_loathed_files():
    files = []
    for root, _, filenames in os.walk(data_root):
        for filename in filenames:
            if filename.endswith(".py"):
                files.append(os.path.join(root, filename))
                logger.info(f"Found file to upload: {filename}")
    return files

def runomni_that_white_boy(command: str):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=config['SFTP']['IP'], username=config['SFTP']['USERNAME'], password=config['SFTP']['PASSWORD'])
    escaped_command = command.replace("'", "'\\''")
    full_command = f"su -l dgadmin -c '{escaped_command}'"
    logger.info(f"Running: {full_command}")
    stdin, stdout, stderr = client.exec_command(full_command)
    error = stderr.read().decode("utf-8", errors="replace")
    logger.info(f"Executed remote command: {full_command}")
    if error and not STUPID_FUCKING_TWC_CORBA_ERROR in error:
        logger.error(f"Error from remote SSH execution: {error}")
    
    client.close()

def sync_that_funky_time_white_boy():
    ntpservers = config['SYSTEM']['NTP_SERVERS']
    ntpnow = None
    freebsd_timestamp = None
    
    for server in ntpservers:
        try:
            ntpnow = ntplib.NTPClient().request(host=server)
            logger.info(f"Queried NTP server: {server}")
            freebsd_timestamp = datetime.fromtimestamp(float(ntpnow.tx_time), tz=timezone.utc).strftime("%Y%m%d%H%M.%S")
            logger.info("Syncing your time... Your timestamp is: " + freebsd_timestamp)
            break
        except Exception as e:
            logger.warning(f"Failed to query {server}: {e}")

    if not ntpnow or freebsd_timestamp is None:
        logger.warning("Could not query any NTP server. Syncing time from host clock instead.")
        utcnow = datetime.now(timezone.utc)
        freebsd_timestamp = utcnow.strftime("%Y%m%d%H%M.%S")
        logger.info("Syncing your time... Your timestamp is: " + freebsd_timestamp)
    
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(hostname=config['SFTP']['IP'], username=config['SFTP']['USERNAME'], password=config['SFTP']['PASSWORD'])
        if not isinstance(freebsd_timestamp, str) or not freebsd_timestamp:
            raise ValueError("freebsd_timestamp is not a valid string")
        command = "date -u " + freebsd_timestamp
        stdin, stdout, stderr = client.exec_command(command)

        output = stdout.read().decode("utf-8", errors="replace")
        error = stderr.read().decode("utf-8", errors="replace")

        if error and not STUPID_FUCKING_TWC_CORBA_ERROR in error:
            logger.error(f"Error from remote time sync execution: {error}")

    except Exception as e:
        logger.error(f"SSH Connection failed: {e}")
    finally:
        client.close()

def sftp_upload(product: str):
    image_root = os.path.join(os.path.dirname(__file__), "radar")
    remote_image_root = "/twc/data/volatile/images/radar/us/"

    sftp_config = config['SFTP']
    transport = paramiko.Transport((sftp_config['IP'], sftp_config['PORT']))
    transport.connect(username=sftp_config['USERNAME'], password=sftp_config['PASSWORD'])
    sftp = paramiko.SFTPClient.from_transport(transport)

    if product == "radar" or product == "all":
        import subprocess
        py_launcher = "py" if os.name == "nt" else "python3"
        
        # shut your mouth you mediocre clarinet player, it's my turn to make it actually work
        # subprocess.run([py_launcher, os.path.join(os.path.dirname(__file__), "radar.py")])
        
        import requests as web
        # either https://weast.9dcrew.org, https://rnwtr.minnwx.com/, or your own rainwater rehost
        weastUrlBase = ""
        # and an api key with access to maps
        weastAPIKey = ""

        wanker74 = web.get(f"{weastUrlBase}/api/maps/timestamps/radar-us?apiKey={weastAPIKey}")
        wanker74.raise_for_status(); wanker74 = wanker74.json()

        os.makedirs("./radar", exist_ok=True)
        
        radardirlist = os.listdir("./radar")
        radar_dir = "./radar"
        for filename in radardirlist:
            file_ts = filename.split('.')[0]
            if file_ts not in wanker74:
                os.remove(os.path.join(radar_dir, filename))

        for ts in wanker74:
            radarname = int(ts) + (3 * 60 * 60)
            radarname = f"./radar/{ts}.{radarname}.tif"
            if not os.path.exists(radarname):
                radardownload = web.get(f"{weastUrlBase}/api/maps/raw/radar-us/{ts}.tiff?apiKey={weastAPIKey}")
                with open(radarname, "wb") as f:
                    f.write(radardownload.content)
            else:
                continue
        
        radardirlist = sorted([f for f in os.listdir("./radar") if f.lower().endswith(".tif")])
        with open("./radarload.py", "w") as f:
            for filename in radardirlist:
                f.write(f"wxdata.setImageData('radar.us', '/twc/data/volatile/images/radar/us/{filename}')\n")

        with os.scandir(image_root) as f:
                try:
                    current_remote_images = sftp.listdir(remote_image_root)

                    for remote_image in current_remote_images:
                        if '.tif' in remote_image:
                            parts = remote_image.split('.')
                            if len(parts) >= 3:
                                expiration_timestamp = int(parts[1])
                                current_timestamp = int(datetime.now(timezone.utc).timestamp())
                                if current_timestamp > expiration_timestamp:
                                    remote_image_path = os.path.join(remote_image_root, remote_image)
                                    sftp.remove(remote_image_path)
                                    logger.info(f"Deleted expired image: {remote_image_path}")
                except FileNotFoundError:
                    return

                for entry in f:
                    if entry.is_file():
                        if '.tif' in entry.name:
                            local_image_path = entry.path
                            remote_image_path = os.path.join(remote_image_root, entry.name)

                            # Ensure remote directory exists
                            try:
                                sftp.stat(remote_image_root)
                            except FileNotFoundError:
                                sftp.mkdir(remote_image_root)

                            try:
                                sftp.stat(remote_image_path)
                                logger.info(f"Skipping upload, file already exists: {remote_image_path}")
                            except FileNotFoundError:
                                sftp.put(local_image_path, remote_image_path)
                                logger.info(f"Uploaded image {local_image_path} to {remote_image_path}")
                sftp.put("./radarload.py","/home/dgadmin/radarslicer.py")
                runomni_that_white_boy("runomni /twc/util/loadSCMTconfig.pyc /home/dgadmin/radarslicer.py")
    if product == "text_data" or product == "all":
        for file_path in enumerate_the_loathed_files():
            remote_path = os.path.join("/home/dgadmin/", os.path.relpath(file_path, data_root))
            remote_dir = os.path.dirname(remote_path)
            try:
                sftp.stat(remote_dir)
            except FileNotFoundError:
                sftp.mkdir(remote_dir)
            sftp.put(file_path, remote_path)
            logger.info(f"Uploaded {file_path} to {remote_path}")
            runomni_that_white_boy(f"runomni /twc/util/loadSCMTconfig.pyc {remote_path}")
    
    sftp.close()
    transport.close()

def set_bulletin(ugc: str, pil: str, pilext: str, bulletinText: str, expirSec: str):
    escaped_text = bulletinText.replace('"', '\\"')
    
    cmd = f'runomni /twc/util/setBulletin.pyc --ugc {ugc} --pil {pil} --pilExt {pilext} --text "{escaped_text}" --dispExpir {expirSec} --expir {expirSec}'
    runomni_that_white_boy(cmd)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', action='store', help="Run a specific job. Available jobs: data, timesync, radar, bulletin", default="data")
    parser.add_argument('--bulletinParams', action='store', help="Parameters for bulletin job in format UGC|PIL|PILEXT|TEXT|EXPIRSEC", default="")
    args = parser.parse_args()
    if args.job == "data":
        sftp_upload("text_data")
    if args.job == "timesync":
        sync_that_funky_time_white_boy()
    if args.job == "radar":
        sftp_upload("radar")
    if args.job == "all":
        sync_that_funky_time_white_boy()
        sftp_upload("all")
    if args.job == "bulletin":
        if args.bulletinParams:
            params = args.bulletinParams.split("|")
            if len(params) == 5:
                set_bulletin(params[0], params[1], params[2], params[3], params[4])
            else:
                logger.error("Invalid bulletinParams format. Expected format: UGC|PIL|PILEXT|TEXT|EXPIRSEC")
        else:
            logger.error("No bulletinParams provided for bulletin job.")
    if args.bulletinParams and args.job != "bulletin":
        logger.error("bulletinParams provided but job is not set to bulletin or is none.")
    if args.job == None:
        logger.error("No job specified. Use --job to specify a job to run.")
        parser.print_help()
