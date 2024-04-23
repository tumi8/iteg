---
layout: page
title: Graph-Parsing Pipeline and Example Data
permalink: /graph-pipeline/
order: 3
toc: Graph-Pipeline
description: "To enable reproducible results and help in understanding our approach we have open-sourced the full parsing pipeline that can create the ITEG from our paper."
---

Additionally, the repository contains an example graph created from the [Tranco Top 1M](https://tranco-list.eu/) list that can be directly imported and explored via [Neo4J](https://neo4j.com/).
The code and data can be found [[here]({{ site.github_link }})] or directly via Git:

```bash
git clone {{ site.github_ssh }}
cd graph-pipeline
git lfs pull
```

Git-LFS is only used for the example data.
For simplicity, we excluded the full IPv4 address space scan (performed with [ZMap](https://zmap.io/)) and the scan for an open port 443 on IPv6 Addresses with [zmapv6](https://github.com/topics/zmapv6) in this example.
In the following we explain the steps necessary to reproduce the `example-data/` from the repository.

## 1. Input Lists

The input for our ITEG was a domain name list and a full IPv4 address space scan.
However, we skipt the latter in this example and, instead, use the IP addresses from the blocklists as additional input. 
For this example, we used the [Tranco](https://tranco-list.eu/) top 1M domains, the [SSLBL](https://sslbl.abuse.ch/), and the [Feodo Tracker](https://feodotracker.abuse.ch/):

```bash
curl https://tranco-list.eu/download/G6Z3K/1000000 | cut -d , -f 2 > example-data/tranco-input.txt
curl https://sslbl.abuse.ch/blacklist/sslblacklist.csv > example-data/blocklists/abuse_ch_sslbl.sha1.csv
curl https://sslbl.abuse.ch/blacklist/sslipblacklist.csv > example-data/blocklists/abuse_ch_sslbl.ip.csv
curl https://feodotracker.abuse.ch/downloads/ipblocklist.csv > example-data/blocklists/abuse_ch_feodo.ip.csv
```

## 2. DNS Resolutions

We have used a local [unbound](https://www.nlnetlabs.nl/projects/unbound/about/) server and [massdns](https://github.com/blechschmidt/massdns) for our large-scale DNS resolutions.
Any software would work that produces a CSV file like the following:

{% highlight bash %}
172.217.16.206,google.com
2a00:1450:4001:806::200e,google.com
{% endhighlight %}

We started with a list of domains names (`example-data/tranco-input.txt`), resolved them via massdns, and parsed the output into the proper format:

```bash
massdns  -o J -t AAAA -t A --filter NOERROR -r ./example-data/resolvers.txt \
    example-data/tranco-input.txt  | ./parse_massdns.py --output example-data/dns
```

This creates two files each for the IPv4 and IPv6 DNS resolutions.

## 3. The TLS Scan

Our TLS measruements were performed with the [TUM goscanner](https://github.com/tumi8/goscanner.git).
Please download and build the scanner.
The scanner needs a single input file:

```bash
cat <(grep -F "#" -v example-data/blocklists/abuse_ch_sslbl.ip.csv | cut -d , --output-delimiter ":" -f 2,3) \
    <(grep -F "#" -v example-data/blocklists/abuse_ch_feodo.ip.csv | csvtool -u : col 2,3 - | tail -n +2) \
    example-data/dns/*.ipdomain | shuf > example-data/goscanner-input.csv
```

Then, we can start with the actual TLS measurements:

```bash
ulimit -n 1024000
goscanner -C example-data/goscanner.conf \
    --input-file example-data/goscanner-input.csv \
    --output example-data/tls
```


## 4. Combining it all as ITEG

We created the ITEG with [Apache Spark](https://spark.apache.org/).
Spark can be tricky to set up; hence, we provide docker scripts that set up the environment and parse the example data.
Have a look at the `docker-compose.yml` and the parsing script for further details.
The script extracts the scan dates from the directory name of the input.
Note the necessary cache directory that can be deleted after the run.

```bash
./create_graph.sh
```

### 5. Explore and Analyze the ITEG

Our parsing pipeline produces multiple files for each type of edge and node that can be analyzed with other tools.
For example, the CSV output under `example-data/tls_graph` can be directly imported into Neo4J.
Although, they need to be decompressed (e.g., with `zstd --rm -d *.zst`).

```bash
./import_neo4j.sh example-data/tls_graph
```


We experienced that Neo4J provides a convenient Interface to manually explore the graph; however, for more advanced analyses we recommend tools like Apache Spark with the [GraphFrames](https://graphframes.github.io/graphframes/) library.

The output of the Neo4J schema function:

![Schema of the Example-Graph]({{site.baseurl}}/assets/schema.svg){:style="display:block; margin-left:auto; margin-right:auto"}

Note that there were IP Addresses embedded as Altertnative Name in some certificates. These were always self-signed certificates.
However, we did not consider such cases in the paper.

#### The TMA website

tma.ifip.org was not on the tranco top list, but we manually added it for demonstration.
If you are running Neo4J you can see yourself, or have a look at the following excerpt:

![Example from the Graph]({{site.baseurl}}/assets/example_ifip_tma.svg){:style="display:block; margin-left:auto; margin-right:auto"}

You can see that tma.ifip.org is quite isolated, it has its own IP address and certificate. However, the parent domain ifip.org has a certificate that reveal the alias ifip.or.at. 
