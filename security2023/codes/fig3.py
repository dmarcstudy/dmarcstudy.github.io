from pyspark import SparkContext, SparkConf
from collections import defaultdict


def extract(v):
    name, tld, txt = v[0], v[1], v[2]
    tags = txt.split(';')
    if 'rua=' in txt or 'ruf=' in txt:
        return name, tld, txt


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


def get_reporting_centrality(i):
    rdd = sc.textFile(dmarc_input_path + '/' + i).map(lambda v: v.strip())
    rdd = rdd.map(process).filter(lambda v: v is not None).map(extract).filter(lambda v: v is not None)
    print(rdd.count())
    reporting_domains = set()
    count, freq = defaultdict(int), defaultdict(int)
    for item in rdd.collect():
        try:
            name, txt = item[0], item[1]
            tags = txt.split(';')
            rua_addresses, ruf_addresses = [], []
            for tag in tags:
                if 'rua=' in tag:
                    rua_addresses = tag.split('rua=')[1].split(',')
                if 'ruf=' in tag:
                    ruf_addresses = tag.split('ruf=')[1].split(',')
            for address in rua_addresses + ruf_addresses:
                if 'mailto:' in address:
                    address = address.split('mailto:')[1]
                    dom = address.split('@')[1]
                    if '!' in dom:
                        dom = dom.split('!')[0]
                    reporting_domains.add(dom)
                    sld = '.'.join(dom.split('.')[-2:])
                    count[sld] += 1
        except Exception as e:
            pass
    print(len(count))  # dmarcian.com appears 50 times as report recievers
    for sld in count:
        freq[count[sld]] += 1
    print(len(freq))  # 50 -> 1

    keys = list(freq.keys())
    keys.sort()
    sorted_freq = {i: freq[i] for i in keys}
    f = open('<YourPathHere>/freq-report-centrality.txt', 'w')
    for item in sorted_freq:
        f.write(str(item) + ' ' + str(sorted_freq[item]) + '\n')
    f.close()


def _gen_cdf_from_freq():
    sorted_freq = defaultdict(int)
    with open('<YourPathHere>/freq-report-centrality.txt') as fin:
        lines = fin.readlines()
        for line in lines:
            x, y = line.split()
            sorted_freq[int(x)] = int(y)
    total = sum(sorted_freq.values())
    print(total, sorted_freq)
    cdf = []
    prev = 0
    for item in sorted_freq:
        prev += sorted_freq[item] / total
        cdf.append((item, prev))
    f = open('<YourPathHere>/cdf-report-centrality.txt', 'w')
    for item in cdf:
        f.write(str(item[0]) + ' ' + str(item[1]) + '\n')
    f.close()


if __name__ == "__main__":
    conf = SparkConf() \
        .setAppName("dmarc-reporting-fig3") \
        # .setMaster("local[*]")

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    dmarc_input_path = "<YourPathHere>"
    # Next two functions generate figure 3 data
    get_reporting_centrality('2023-01-08')
    _gen_cdf_from_freq()

