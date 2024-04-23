---
layout: page
title: Probabilistic Threat Propagation (PTP)
permalink: /ptp/
order: 4
toc: PTP
---

For our paper, we combined our ITEG with a Probabilistic Threat Propagation (PTP) approach, defined by Carter et al. [DOI:10.1109/ICASSP.2013.6638196](https://doi.org/10.1109/ICASSP.2013.6638196).

## Message-passing based implementation of PTP on the ITEgraph

To enable reproducible results and help in understanding our approach we have open-sourced the PTP algorithm used in our paper.
The code and pre-computed data can be found [[here]({{ site.github_link }})] or directly via Git:

```bash
git clone {{ site.github_ssh }}
cd graph-pipeline
git lfs pull
```

Git-LFS is only used for the example data.

### Running the algorithm

The PTP algorithm can be run with docker and spark in the provided container.

```bash
./run_ptp.sh
```

### Results

The following tables show the IP addresses and domains with a score of 100% found with the blocked SSLBL certificates as input.
We checked each entry with Virus Total and appended the aggregated class (according to the paper).
Interestingly, even when scanning just the Tranco Top 1 Million websites, we found several Domains and IP addresses with a high threat score and also Virus Total identifies them as potentially malicious.
Just a high threat score does not mean that these addresses and domains are actually malicious, they are just somehow related to malicious entries through the ITEG; however, they can be a good starting point for a more thorough analysis searching for unknown threats. 

| Domain              | VT class   |
|---------------------|------------|
| uni.me              | malicious  |
| assortedrent.best   | malicious  |
| igoseating.com      | malicious  |
| cinemacity.live     | malicious  |
| avstop.com          | harmless   |
| monnalisa.com       | harmless   |
| manyhit.com         | harmless   |
| ccdcn.cn            | harmless   |
| ilkconstruction.com | harmless   |
| eglobaldomains.com  | harmless   |
| eflowsys.com        | harmless   |
| imbroadbandmpl.com  | harmless   |
| 7-live.com          | harmless   |
| itlalaguna.edu.mx   | harmless   |
| surtitodo.com.co    | harmless   |
| ikoop.com.my        | harmless   |
| ucflower.tw         | harmless   |
| lamolina.edu.pe     | harmless   |
| neunet.com.ar       | harmless   |
| unipol.edu.bo       | undetected |
| kcmservice.com      | undetected |


| IP Address      | VT class   |
|-----------------|------------|
| 104.243.46.129  | malicious  |
| 45.145.55.81    | malicious  |
| 216.218.135.114 | malicious  |
| 185.16.39.253   | malicious  |
| 82.222.185.244  | malicious  |
| 80.79.7.197     | malicious  |
| 103.145.57.203  | malicious  |
| 20.26.126.28    | malicious  |
| 203.188.15.2    | harmless   |
| 203.174.41.164  | harmless   |
| 104.243.37.63   | harmless   |
| 40.90.180.148   | harmless   |
| 59.92.232.2     | harmless   |
| 45.81.115.161   | undetected |
| 121.4.202.96    | undetected |
| 190.14.231.210  | suspicious |
| 45.231.83.134   | undetected |
| 78.46.205.169   | undetected |
| 200.59.236.49   | undetected |
| 187.190.56.90   | undetected |
| 125.229.114.79  | undetected |
| 114.32.146.202  | undetected |
| 200.105.167.174 | undetected |
| 80.211.143.18   | undetected |
| 202.57.128.136  | undetected |
| 103.149.103.38  | undetected |

