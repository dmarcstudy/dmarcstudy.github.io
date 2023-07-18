from pyspark import SparkContext, SparkConf
from pyspark.accumulators import AccumulatorParam
import subprocess
import json
import datetime
import os
from collections import defaultdict
from subprocess import run


def upload_dmarc_records_to_hdfs(i):
    run("hdfs" + " dfs" + " -put " + dir + i + '/DMARC' + ' ' + dmarc_input_path, shell=True)
    run("hdfs" + " dfs" + " -mv " + dmarc_input_path + 'DMARC' + ' ' + dmarc_input_path + i, shell=True)


def process(v):
    import json
    v = json.loads(v)
    for answer in v["data"].get("answers", []):
        d = answer.get('answer')
        if d and not isinstance(d, str):
            d = d.decode()
        if d and d.startswith('v=DMARC1'):
            name = v['name'][:-1] if v['name'][-1] == '.' else v['name']
            tld = name.split('.')[-1]
            return v['name'], tld, d
    return


def extract(v):
    name, tld, txt = v[0], v[1], v[2]
    tags = txt.split(';')
    if 'rua=' in txt or 'ruf=' in txt:
        return name, tld, txt


def check_reporting_presence(v):
    name, tld, txt = v[0], v[1], v[2]
    rua, ruf = False, False
    if 'rua=' in txt:
        if '@' in txt.split('rua=')[1].split(';')[0]:
            rua = True
    if 'ruf=' in txt:
        if '@' in txt.split('ruf=')[1].split(';')[0]:
            ruf = True
    tags = txt.split(';')
    sld_in_reporting = set()
    try:
        rua_addresses, ruf_addresses = [], []
        for tag in tags:
            if 'rua=' in tag:
                rua_addresses = tag.split('rua=')[1].split(',')
            if 'ruf=' in tag:
                ruf_addresses = tag.split('ruf=')[1].split(',')
        for address in rua_addresses + ruf_addresses:
            try:
                if 'mailto:' in address:
                    address = address.split('mailto:')[1]
                    dom = address.split('@')[1]
                    if '!' in dom:
                        dom = dom.split('!')[0]
                    sld = '.'.join(dom.split('.')[-2:])
                    sld_in_reporting.add(sld)
            except Exception as e:
                continue
    except Exception as e:
        pass
    external = False
    for sld in sld_in_reporting:
        if sld not in name:
            external = True
    return name, tld, txt, rua, ruf, external


def get_num_enabled_domains(i):
    rdd = sc.textFile(dmarc_input_path + '/' + i).map(lambda v: v.strip())
    rdd = rdd.map(process).filter(lambda v: v is not None).distinct()
    dcom, dnet, dorg, dse = rdd.filter(lambda v: v[1] == 'com').count(), rdd.filter(
        lambda v: v[1] == 'net').count(), rdd.filter(lambda v: v[1] == 'org').count(), rdd.filter(
        lambda v: v[1] == 'se').count()
    dtotal = dcom + dnet + dorg + dse
    rdd = rdd.map(check_reporting_presence)
    repcom, repnet, reporg, repse = rdd.filter(lambda v: v[4] or v[3]).filter(
        lambda v: v[1] == 'com').count(), rdd.filter(lambda v: v[4] or v[3]).filter(
        lambda v: v[1] == 'net').count(), rdd.filter(lambda v: v[4] or v[3]).filter(
        lambda v: v[1] == 'org').count(), rdd.filter(lambda v: v[4] or v[3]).filter(
        lambda v: v[1] == 'se').count()  # or
    extrepcom, extrepnet, extreporg, extrepse = rdd.filter(lambda v: v[5]).filter(
        lambda v: v[1] == 'com').count(), rdd.filter(lambda v: v[5]).filter(
        lambda v: v[1] == 'net').count(), rdd.filter(lambda v: v[5]).filter(
        lambda v: v[1] == 'org').count(), rdd.filter(lambda v: v[5]).filter(lambda v: v[1] == 'se').count()  # or
    res = str(i) + ", " + str(dtotal) + ', ' + str(dcom) + ', ' + str(dnet) + ', ' + str(dorg) + ', ' + str(
        dse) + ', ' + str(repcom) + ', ' + str(repnet) + ', ' + str(reporg) + ', ' + str(repse) + ', ' + str(
        extrepcom) + ', ' + str(extrepnet) + ', ' + str(extreporg) + ', ' + str(extrepse)
    return res


class DictParam(AccumulatorParam):
    def zero(self, value):
        print('zero', value)
        return value

    def addInPlace(self, value1, value2):
        for key in value2.keys():
            # print('key', key)
            value1[key] += value2[key]
        return value1


def unzip_dns_crawls(fp, dt):
    p = subprocess.run("mkdir " + "<YourPathHere>" + dt, shell=True, capture_output=True)
    p = subprocess.run("mkdir " + "<YourPathHere>" + dt + "/dns", shell=True, capture_output=True)
    p = subprocess.run("unzip " + fp + '/dns.zip' + ' -d ' + ' <YourPathHere>' + dt + "/dns/", shell=True,
                       capture_output=True)
    output, err = p.stdout, p.stderr
    print("Step1: ", output, err)


def upload_crawled_records_to_hdfs(dt):
    subprocess.Popen(["hadoop", "fs", "-mkdir", input_path_mx + dt], stdin=subprocess.PIPE, bufsize=-1)
    put = subprocess.Popen(["hadoop", "fs", "-put", "<YourPathHere>" + dt + "/dns/", input_path_mx + dt + '/'],
                           stdin=subprocess.PIPE, bufsize=-1)
    put.communicate()


def compute_number_of_mx_records(dt):
    temp_hdfs_path = "<YourPathHere>"
    rdd = sc.textFile(temp_hdfs_path + dt + '/dns/')

    def extract_mx(v):
        try:
            v = json.loads(v)
            global tld_accum
            name = v[0][:-1] if v[0][-1] == '.' else v[0]
            tld = name.split('.')[-1]
            for records in v[1]:
                if 'data' in records:
                    for exchange in records['data'].get('exchanges', []):
                        if exchange['type'] == 'MX':
                            tld_accum += {tld: 1}
                            return
            return
        except Exception as e:
            return

    rdd.foreach(extract_mx)


if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("dmarc-reporting-fig2") \
        # .setMaster("local[*]")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    st_date = "2021-09-19"
    dates_v3 = [st_date]
    while st_date < "2023-01-09":
        dt = datetime.datetime.strptime(st_date, '%Y-%m-%d')
        dt = datetime.timedelta(days=1) + dt
        st_date = dt.strftime('%Y-%m-%d')
        dates_v3.append(st_date)

    # This block finds out the number of domains with MX records per date
    input_path_mx = "hdfs://<YourPathHere>/dns-records-by-date/"
    dmx = {}
    for date in dates_v3:
        if os.path.exists('<YourPathHere>' + date + '/dns.zip'):
            unzip_dns_crawls('<YourPathHere>' + date, date)
            upload_crawled_records_to_hdfs(date)
            tld_accum = sc.accumulator(defaultdict(int), DictParam())
            compute_number_of_mx_records(date)  # per tld
            dmx[date] = str(tld_accum.value['com']) + ', ' + str(tld_accum.value['net']) + ', ' + str(
                tld_accum.value['org']) + ', ' + str(tld_accum.value['se'])

    # This block generates the csv file for Figure 2
    fout = open('dmarc-deployment-with-tld-mx.csv', 'w')
    dmarc_input_path = "<YourPathHere>"
    tld_accum = sc.accumulator(defaultdict(int), DictParam())
    dir = "<YourPathHere>"
    for i in os.listdir(dir):
        if os.path.exists(dir + i + '/DMARC'):
            upload_dmarc_records_to_hdfs(i)
            res = get_num_enabled_domains(i)
            res = res + ', ' + dmx[i]
            fout.write(res + '\n')


