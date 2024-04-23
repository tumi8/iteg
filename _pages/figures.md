---
layout: page
title: Figures
permalink: /figures/
order: 2
toc: Figures
description: "Interactive versions of the figures from our paper."
---

<script charset="utf-8" src="{{ site.baseurl }}{% link assets/plotly.min.js %}"></script>

### Figure 4

{% include cdf_edges.html %}

Nodes ordered by the fraction of edges they accumulate. The figure shows a high centralization on IP addresses (90% of the edges congregate around only 2% of the addresses). The legend also lists the total number of nodes.

### Figure 5

{% include ptp_scores_domains.html %}

{% include ptp_scores_ip_addresses.html %}

Cumulative number of domains and IP addresses with a threat probability above the depicted value, found via PTP. Categorized into the Virus Total and Google Safe Browsing labels.

### Figure 6

{% include over_time_find_rate.html %}

Rate of nodes with a score above the depicted threshold that appeared later on the blocklist. Marked are the s e thresholds providing the maximal rate.

### Figure 7

<div style="text-align: center;">abuse.ch Feodo</div>
{% include over_time_abuse_ch_feodo_ip.html %}

<div style="text-align: center;">Blocklist.de Strongips</div>
{% include over_time_blocklist_de_strongips_ip.html %}

<div style="text-align: center;">Openphish</div>
{% include over_time_openphish_dn.html %}


Number of nodes identified using an ITEG and the described blocklist as PTP input on each measurement date. Also, showing the percentage of identified nodes that appeared later on the respective blocklist as Appearance Rate.

