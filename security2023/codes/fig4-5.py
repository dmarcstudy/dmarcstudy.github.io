from pyspark import SparkContext, SparkConf
import json
from subprocess import run


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


def _crawl_dns():
    with open('<YourPathHere>/alexa-top-1m.csv') as fin:
        lines = fin.readlines()
        fout = open('<YourPathHere>/top-1m-dmarc.txt', 'w')
        for i in lines:
            fout.write('_dmarc.' + i)
        fout.close()
    run(
        'cat <YourPathHere>/alexa-top-1m.csv | <YourPathHere>/zdns/zdns/./zdns TXT --name-servers=1.1.1.1,8.8.8.8 | '
        'grep -v -E "NXDOMAIN|SERVFAIL" > <YourPathHere>/top-1m-dmarc.txt',
        shell=True)


def analysis_alexa():
    f = open('<YourPathHere>/alexa-top-1m.csv')  # TODO: path to top 1m domains
    top_1m = f.readlines()
    f.close()

    dmarc = {i.strip(): [False, False, False, None] for i in top_1m}
    to_scan = set()

    f = open('<YourPathHere>/top-1m-dmarc.txt')  # TODO: path to dmarc record of top 1m domains
    lines = f.readlines()
    for line in lines:
        try:
            x = process(line)
            if x:
                name_sld = '.'.join(x[0].split('.')[-2:])
                domain = '.'.join(x[0].split('.')[1:])
                dmarc[domain][0] = True

                if 'rua=' in x[1]:
                    dmarc[domain][1] = True
                if 'ruf=' in x[1]:
                    dmarc[domain][1] = True

                tags = x[1].split(';')
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
                        sld = '.'.join(dom.split('.')[-2:])
                        if sld != name_sld:
                            dmarc[domain][2] = True
                            dmarc[domain][3] = False
                            auth = '.'.join(x[0].split('.')[1:]) + '._report._dmarc.' + dom
                            to_scan.add(auth)
                            # import dns.resolver
                            # try:
                            #     answers = dns.resolver.resolve(auth, "TXT").rrset
                            #     flag = False
                            #     for ans in answers:
                            #         if ans.to_text().startswith("v=DMARC1"):
                            #             flag = True
                            #     if not flag:
                            #         dmarc[domain][3] = True
                            # except Exception as e:
                            #     dmarc[domain][3] = True
        except Exception as e:
            print(e)
    f.close()

    fout = open('<YourPathHere>/top-1m-edv-check.txt', 'w')
    for i in to_scan:
        fout.write(i + '\n')
    fout.close()

    run(
        'cat <YourPathHere>/top-1m-edv-check.txt | <YourPathHere>/zdns/zdns/./zdns TXT --name-servers=1.1.1.1,8.8.8.8 > '
        '<YourPathHere>/top-1m-edv-result.txt',
        shell=True)

    f = open('<YourPathHere>/top-1m-edv-result.txt')
    lines = f.readlines()
    for line in lines:
        try:
            v = json.loads(line)
            for answer in v["data"].get("answers", []):
                d = answer.get('answer')
                if d and not isinstance(d, str):
                    d = d.decode()
                if d and d.startswith('v=' + 'DMARC1'):
                    dmarc[v['name'].split('._report')[0]][3] = True
                    break
        except Exception as e:
            # print(e, v)
            continue

    # print(dmarc)
    # print(len(dmarc))
    json.dump(dmarc, open('<YourPathHere>/top-1m-stats.json', 'w'), indent=4)


def analysis_with_bins():
    stats = json.load(open('<YourPathHere>/top-1m-stats.json'))
    res = {i: [0, 0, 0, 0] for i in range(1, 101)}
    with open('<YourPathHere>/alexa-top-1m.csv') as f:
        top_1m = f.readlines()
        i = 1
        while i < 101:
            cur_bin = top_1m[(i - 1) * 10000: i * 10000]
            dmarc, reporting, external, auth = 0, 0, 0, 0
            print(cur_bin[0: 5])
            for dom in cur_bin:
                dom = dom.strip()
                if stats[dom][0]:
                    dmarc += 1
                if stats[dom][1]:
                    reporting += 1
                if stats[dom][2]:
                    external += 1
                    if not stats[dom][3]:
                        auth += 1
            res[i] = [dmarc, reporting, external, auth]
            i += 1
    fout = open('<YourPathHere>/top-1m-result.txt', 'w')
    for i in res:
        fout.write(str(i) + ' ' + " ".join([str(j) for j in res[i]]) + '\n')
    fout.close()
    # json.dump(res, open('<YourPathHere>/top-1m-result.json', 'w'), indent=4)


if __name__ == "__main__":
    # Next three functions generate figure 4 and 5 data
    _crawl_dns()
    analysis_alexa()
    analysis_with_bins()

