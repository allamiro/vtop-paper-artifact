# Summary differences: `research/analysis/v0102/summary.json` → `research/analysis/v0104/summary.json`

2095 leaves equal; 1141 changed, 64 added, 37 removed (exact).

| Kind | Key | Old | New |
|---|---|---|---|
| changed | `batch_sizes.crash.file-mix.batches` | 1970 | 1965 |
| changed | `batch_sizes.crash.hdfs.batches` | 473 | 474 |
| changed | `batch_sizes.crash.power-loss.batches` | 1969 | 1963 |
| changed | `batch_sizes.crash.spool-mix.batches` | 740 | 737 |
| changed | `batch_sizes.kafka_rq1.hdfs-p4.batches` | 497 | 474 |
| changed | `batch_sizes.kafka_rq1.hdfs-p4.batches_over_setting` | 18 | 51 |
| changed | `batch_sizes.kafka_rq1.hdfs-p4.max_mib` | 4.031 | 4.032 |
| changed | `batch_sizes.kafka_rq1.hdfs-p4.median_mib` | 3.195 | 3.217 |
| changed | `batch_sizes.kafka_rq1.ssh-p1.batches_over_setting` | 136 | 134 |
| changed | `batch_sizes.kafka_rq4_vtop.vtop-hdfs-p4-kill.batches` | 400 | 388 |
| changed | `batch_sizes.kafka_rq4_vtop.vtop-hdfs-p4-kill.batches_over_setting` | 15 | 18 |
| changed | `batch_sizes.kafka_rq4_vtop.vtop-hdfs-p4-kill.max_mib` | 4.031 | 4.032 |
| changed | `batch_sizes.kafka_rq4_vtop.vtop-hdfs-p4-kill.median_mib` | 3.319 | 3.493 |
| expected | `generated_from_commit` | "03d6cf9" | "eb15ebf" |
| expected | `result_set` | "v0102" | "v0104" |
| changed | `rq1_crash.file-mix.final_drain_s_median_ci` | [0.002, 0.002, 1.056] | [9.5125, 8.528, 13.549] |
| changed | `rq1_crash.file-mix.first_commit_after_restart_s_median_ci` | [0.049341559410095215, 0.03054642677307129, 0.05120384693145752] | [0.130560040473938, 0.11528730392456055, 0.14769256114959717] |
| changed | `rq1_crash.file-mix.kills` | 72 | 80 |
| changed | `rq1_crash.file-mix.manifests` | 1972 | 1987 |
| changed | `rq1_crash.hdfs.final_drain_s_median_ci` | [1.057, 1.041, 1.968] | [13.406, 11.391, 15.271] |
| changed | `rq1_crash.hdfs.first_commit_after_restart_s_median_ci` | [0.08313894271850586, 0.07671403884887695, 0.09058618545532227] | [0.25030362606048584, 0.19709300994873047, 0.27583205699920654] |
| changed | `rq1_crash.hdfs.manifests` | 475 | 479 |
| changed | `rq1_crash.kafka-hdfs-p4.final_drain_s_median_ci` | [6.054, 2.3955, 8.235] | [11.823, 7.4845, 13.3825] |
| changed | `rq1_crash.kafka-hdfs-p4.first_commit_after_restart_s_median_ci` | [NaN, NaN, NaN] | [NaN, NaN, NaN] |
| changed | `rq1_crash.kafka-hdfs-p4.manifests` | 497 | 475 |
| changed | `rq1_crash.kafka-ssh-p1.final_drain_s_median_ci` | [4.4925, 4.126, 7.721] | [0.002, 0.002, 0.002] |
| changed | `rq1_crash.kafka-ssh-p1.first_commit_after_restart_s_median_ci` | [NaN, NaN, NaN] | [NaN, NaN, NaN] |
| changed | `rq1_crash.kafka-ssh-p1.kills` | 32 | 21 |
| changed | `rq1_crash.kafka-ssh-p1.manifests` | 144 | 145 |
| changed | `rq1_crash.kafka-ssh-p1.outages` | 8 | 6 |
| changed | `rq1_crash.power-loss.final_drain_s_median_ci` | [0.002, 0.002, 1.064] | [7.7855, 7.166, 10.583] |
| changed | `rq1_crash.power-loss.first_commit_after_restart_s_median_ci` | [0.0511629581451416, 0.04956388473510742, 0.05304694175720215] | [0.11943817138671875, 0.11119985580444336, 0.1315479278564453] |
| changed | `rq1_crash.power-loss.kills` | 69 | 80 |
| changed | `rq1_crash.power-loss.manifests` | 1971 | 1988 |
| changed | `rq1_crash.power-loss.outages` | 8 | 10 |
| changed | `rq1_crash.power-loss.power_cut_dropped_commits_total` | 137 | 408 |
| changed | `rq1_crash.power-loss.power_cuts` | 69 | 80 |
| changed | `rq1_crash.power-loss.power_cuts_dropping_commits` | 23 | 47 |
| changed | `rq1_crash.power-loss.power_cuts_empty_wal` | 16 | 1 |
| changed | `rq1_crash.spool-mix.final_drain_s_median_ci` | [0.001, 0.001, 0.0015] | [1.0835, 0.058, 1.107] |
| changed | `rq1_crash.spool-mix.first_commit_after_restart_s_median_ci` | [1.0312628746032715, 1.0285871028900146, 1.046396255493164] | [0.0746309757232666, 0.07266998291015625, 0.0805971622467041] |
| changed | `rq1_crash.spool-mix.kills` | 47 | 78 |
| changed | `rq1_crash.spool-mix.manifests` | 740 | 751 |
| changed | `rq1_crash.spool-mix.outages` | 5 | 10 |
| changed | `rq2_norecheck.patched_source_sha256` | "97ba510896894777bb88056be0fdafcaa70a703c8f2d8ac460bf5817fb605459" | "8d27211f5279285308e6b20519198333499be35f324b0b82d3fd0c875f141ef7" |
| changed | `rq3.batch_sweep.datasets.bgl.1` | [158.9, 148.48, 159.69] | [36.72, 36.31, 37.22] |
| changed | `rq3.batch_sweep.datasets.bgl.16` | [345.64, 339.38, 352.54] | [229.1, 226.99, 245.17] |
| changed | `rq3.batch_sweep.datasets.bgl.4` | [277.3, 265.24, 287.98] | [111.04, 110.22, 115.09] |
| changed | `rq3.batch_sweep.datasets.bgl.64` | [362.43, 349.28, 367.07] | [310.57, 302.15, 324.35] |
| changed | `rq3.batch_sweep.datasets.hdfs.1` | [155.52, 152.92, 157.97] | [36.51, 36.21, 36.84] |
| changed | `rq3.batch_sweep.datasets.hdfs.16` | [347.85, 337.88, 354.24] | [228.59, 226.14, 234.93] |
| changed | `rq3.batch_sweep.datasets.hdfs.4` | [281.26, 270.52, 282.79] | [108.96, 106.63, 111.77] |
| changed | `rq3.batch_sweep.datasets.hdfs.64` | [362.36, 350.1, 366.2] | [311.31, 307.65, 320.15] |
| changed | `rq3.batch_sweep.manifest_overhead_pct.1` | [2.185972915520641, 2.1859684967535538, 2.185972915520641] | [2.185977334287728, 2.1859684967535538, 2.185977334287728] |
| changed | `rq3.batch_sweep.manifest_overhead_pct.16` | [0.13406895761214885, 0.1340646933323266, 0.13406895761214885] | [0.13406895761214885, 0.13406895761214885, 0.13406895761214885] |
| changed | `rq3.cpu_s_per_gib.bgl|blake3` | [3.015553114730052, 2.968886592304898, 3.0708883410112713] | [3.8131461086868956, 3.665127990344574, 3.9992342847971054] |
| changed | `rq3.cpu_s_per_gib.bgl|sha256` | [2.9904138735474612, 2.9330559496998263, 3.0098462180248085] | [3.8174804606149286, 3.663827684766164, 3.8876247226502603] |
| changed | `rq3.cpu_s_per_gib.bgl|sha256+mac` | [2.9515491845927664, 2.9281436841813893, 2.9920753751198736] | [3.9116803758508425, 3.6883167731595496, 4.005302377496352] |
| changed | `rq3.cpu_s_per_gib.bgl|sha256-batch1` | [5.549126295062851, 5.482955188961549, 5.938495576597801] | [6.318040327095878, 5.954532678731524, 6.5285453524006725] |
| changed | `rq3.cpu_s_per_gib.bgl|sha256-batch16` | [2.899681439853974, 2.8762759394425963, 2.9788556017393732] | [3.0456046214310795, 2.888990038431493, 3.0977613229650744] |
| changed | `rq3.cpu_s_per_gib.bgl|sha256-batch4` | [3.446676653171719, 3.3514653891525974, 3.6008351034120873] | [3.7496478529412145, 3.524261552683506, 3.784756103558281] |
| changed | `rq3.cpu_s_per_gib.bgl|sha256-batch64` | [2.8465133895367707, 2.8044701758348523, 2.917018847566105] | [2.956750406906406, 2.7900223360747427, 3.004428278114767] |
| changed | `rq3.cpu_s_per_gib.bgl|sha256-nocomp` | [3.0633754643360143, 3.052828541311134, 3.1220336937620585] | [4.437653982317628, 4.3300175761048125, 4.705516931470058] |
| changed | `rq3.cpu_s_per_gib.bgl|size-only` | [2.8289592642282377, 2.8252028258906092, 2.903582356589203] | [3.659782289633333, 3.5080077329533834, 3.967665754921266] |
| changed | `rq3.cpu_s_per_gib.hdfs|blake3` | [2.9575511453131043, 2.9217253408518227, 3.029066663810362] | [3.738301915282218, 3.6764828395878704, 3.843601881861197] |
| changed | `rq3.cpu_s_per_gib.hdfs|sha256` | [2.9112123554970877, 2.883245773097114, 2.9693910123145533] | [3.7976713633210926, 3.674169302357703, 3.9325369747973684] |
| changed | `rq3.cpu_s_per_gib.hdfs|sha256+mac` | [2.9202623687798046, 2.8924318768051345, 2.9627906266873083] | [3.7222092224900187, 3.673454827624856, 3.7918534976393463] |
| changed | `rq3.cpu_s_per_gib.hdfs|sha256-batch1` | [5.680822623465351, 5.612845456025998, 5.7573734876988585] | [6.513764071538554, 6.183404564113066, 6.573099496971103] |
| changed | `rq3.cpu_s_per_gib.hdfs|sha256-batch16` | [2.902400500458653, 2.869806843598343, 2.980788585433764] | [3.080270686330996, 2.9795637716060277, 3.1165387846745154] |
| changed | `rq3.cpu_s_per_gib.hdfs|sha256-batch4` | [3.428730220321918, 3.4107662841817885, 3.5450194887441957] | [3.7955619617288807, 3.71445206824769, 3.86238236055315] |
| changed | `rq3.cpu_s_per_gib.hdfs|sha256-batch64` | [2.806184569768717, 2.7863834128869835, 2.8770196361394555] | [2.883551976554048, 2.847488013848485, 2.968812628007011] |
| changed | `rq3.cpu_s_per_gib.hdfs|sha256-nocomp` | [3.135999715492989, 3.097486125131954, 3.1687294627786033] | [4.6186028313612155, 4.498434985817774, 4.745064859074969] |
| changed | `rq3.cpu_s_per_gib.hdfs|size-only` | [2.803156557805703, 2.7869277745881993, 2.8428609393881485] | [3.6698824539606263, 3.6204816295752704, 3.801515917835931] |
| changed | `rq3.cpu_s_per_gib.ssh|blake3` | [3.338189777105888, 3.2394700356142576, 3.4076592248222215] | [3.8412948300410146, 3.6833432436544054, 3.896139130869698] |
| changed | `rq3.cpu_s_per_gib.ssh|sha256` | [3.3703651002587165, 3.243857579680552, 3.4135092835772807] | [3.885170270703962, 3.7754816690465938, 4.02191539410348] |
| changed | `rq3.cpu_s_per_gib.ssh|sha256+mac` | [3.236545006236728, 3.12685640457936, 3.398884136689632] | [3.864695065061253, 3.7498876619932084, 3.989740070950652] |
| changed | `rq3.cpu_s_per_gib.ssh|sha256-nocomp` | [3.686999530376318, 3.608754994527395, 3.8157008229876292] | [4.965237368356842, 4.8613988254545335, 5.28699059988512] |
| changed | `rq3.cpu_s_per_gib.ssh|size-only` | [3.191938308229398, 3.0705495890619114, 3.3301459463176815] | [3.5707296126195076, 3.454459694862698, 3.733800000416794] |
| changed | `rq3.mac_drop_vs_sha256_ci.bgl` | [-1.0, -3.0, 1.4] | [1.2, -4.3, 7.6] |
| changed | `rq3.mac_drop_vs_sha256_ci.hdfs` | [0.1, -1.8, 2.7] | [-2.2, -6.0, 1.0] |
| changed | `rq3.mac_drop_vs_sha256_ci.ssh` | [-0.4, -6.1, 3.6] | [-2.8, -6.8, 2.7] |
| changed | `rq3.max_rss_mib.bgl|blake3` | [172.35000000000002, 171.8, 173.4] | [174.7, 174.0, 174.8] |
| changed | `rq3.max_rss_mib.bgl|sha256` | [172.55, 171.5, 173.4] | [174.35000000000002, 173.55, 174.60000000000002] |
| changed | `rq3.max_rss_mib.bgl|sha256+mac` | [172.55, 171.75, 173.10000000000002] | [174.2, 173.45, 174.7] |
| changed | `rq3.max_rss_mib.bgl|sha256-batch1` | [39.2, 38.6, 39.8] | [39.6, 39.2, 39.8] |
| changed | `rq3.max_rss_mib.bgl|sha256-batch16` | [172.4, 170.2, 174.3] | [174.2, 171.6, 174.6] |
| changed | `rq3.max_rss_mib.bgl|sha256-batch4` | [76.9, 76.1, 79.7] | [78.3, 77.1, 78.4] |
| changed | `rq3.max_rss_mib.bgl|sha256-batch64` | [389.6, 389.6, 390.7] | [389.9, 389.7, 393.8] |
| changed | `rq3.max_rss_mib.bgl|sha256-nocomp` | [169.5, 168.65, 170.1] | [170.95, 170.0, 171.65] |
| changed | `rq3.max_rss_mib.bgl|size-only` | [171.64999999999998, 170.89999999999998, 172.5] | [172.35000000000002, 172.05, 173.6] |
| changed | `rq3.max_rss_mib.hdfs|blake3` | [113.9, 112.8, 114.45] | [115.3, 114.95, 115.4] |
| changed | `rq3.max_rss_mib.hdfs|sha256` | [113.75, 113.0, 114.9] | [115.15, 114.8, 115.30000000000001] |
| changed | `rq3.max_rss_mib.hdfs|sha256+mac` | [113.45, 112.9, 114.4] | [115.1, 114.6, 115.30000000000001] |
| changed | `rq3.max_rss_mib.hdfs|sha256-batch1` | [29.8, 29.6, 30.4] | [30.3, 30.0, 30.4] |
| changed | `rq3.max_rss_mib.hdfs|sha256-batch16` | [113.8, 111.8, 114.4] | [114.1, 113.1, 115.0] |
| changed | `rq3.max_rss_mib.hdfs|sha256-batch4` | [51.0, 50.6, 52.2] | [50.8, 50.6, 52.2] |
| changed | `rq3.max_rss_mib.hdfs|sha256-batch64` | [620.0, 617.0, 620.7] | [617.6, 617.5, 620.0] |
| changed | `rq3.max_rss_mib.hdfs|sha256-nocomp` | [113.4, 112.2, 113.7] | [114.6, 114.1, 114.65] |
| changed | `rq3.max_rss_mib.hdfs|size-only` | [112.2, 111.45, 113.0] | [113.5, 112.85, 113.55] |
| changed | `rq3.max_rss_mib.ssh|blake3` | [83.6, 83.6, 83.7] | [84.9, 84.4, 85.5] |
| changed | `rq3.max_rss_mib.ssh|sha256` | [85.25, 83.8, 85.7] | [84.1, 84.0, 84.9] |
| changed | `rq3.max_rss_mib.ssh|sha256+mac` | [83.8, 83.65, 83.94999999999999] | [85.25, 84.0, 85.55] |
| changed | `rq3.max_rss_mib.ssh|sha256-nocomp` | [83.5, 83.5, 84.25] | [84.9, 83.9, 85.7] |
| changed | `rq3.max_rss_mib.ssh|size-only` | [82.6, 82.5, 82.7] | [82.9, 82.85, 83.0] |
| changed | `rq3.stage_mean_ms_per_batch.bgl|blake3.checksum_ms` | 0.21481481481481482 | 0.2962962962962963 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|blake3.compress_ms` | 18.25925925925926 | 18.562962962962963 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|blake3.manifest_upload_ms` | 0.0 | 10.303703703703704 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|blake3.object_upload_ms` | 0.0 | 13.251851851851852 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|blake3.state_write_ms` | 0.9851851851851852 | 1.037037037037037 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|blake3.verify_ms` | 0.8 | 0.4444444444444444 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256.checksum_ms` | 0.0 | 0.014814814814814815 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256.compress_ms` | 18.103703703703705 | 18.474074074074075 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256.manifest_upload_ms` | 0.0 | 10.74074074074074 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256.object_upload_ms` | 0.0 | 13.185185185185185 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256.state_write_ms` | 1.0074074074074073 | 1.0148148148148148 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256.verify_ms` | 0.3111111111111111 | 0.05185185185185185 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256+mac.checksum_ms` | 0.0 | 0.007407407407407408 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256+mac.compress_ms` | 18.28148148148148 | 18.666666666666668 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256+mac.manifest_upload_ms` | 0.0 | 9.518518518518519 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256+mac.object_upload_ms` | 0.037037037037037035 | 12.007407407407408 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256+mac.verify_ms` | 0.6148148148148148 | 0.2074074074074074 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256-nocomp.checksum_ms` | 5.874074074074074 | 5.925925925925926 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256-nocomp.compress_ms` | 8.303703703703704 | 8.22962962962963 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256-nocomp.manifest_upload_ms` | 0.0 | 10.014814814814814 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256-nocomp.object_upload_ms` | 0.0 | 19.674074074074074 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256-nocomp.state_write_ms` | 1.037037037037037 | 1.0148148148148148 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|sha256-nocomp.verify_ms` | 7.148148148148148 | 5.977777777777778 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|size-only.commit_ms` | 0.02962962962962963 | 0.0 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|size-only.compress_ms` | 18.496296296296297 | 18.822222222222223 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|size-only.manifest_upload_ms` | 0.0 | 7.2 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|size-only.object_upload_ms` | 0.0 | 10.481481481481481 |
| changed | `rq3.stage_mean_ms_per_batch.bgl|size-only.state_write_ms` | 0.9259259259259259 | 1.0074074074074073 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|blake3.checksum_ms` | 0.13333333333333333 | 0.17192982456140352 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|blake3.commit_ms` | 0.007017543859649123 | 0.0 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|blake3.compress_ms` | 17.971929824561403 | 18.266666666666666 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|blake3.manifest_upload_ms` | 0.0035087719298245615 | 10.389473684210527 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|blake3.object_upload_ms` | 0.0035087719298245615 | 12.992982456140352 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|blake3.state_write_ms` | 1.0070175438596491 | 1.0315789473684212 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|blake3.verify_ms` | 0.8456140350877193 | 0.3157894736842105 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256.compress_ms` | 17.79298245614035 | 18.2140350877193 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256.manifest_upload_ms` | 0.03859649122807018 | 10.350877192982455 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256.object_upload_ms` | 0.014035087719298246 | 13.04561403508772 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256.state_write_ms` | 0.9929824561403509 | 1.0105263157894737 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256.verify_ms` | 0.3543859649122807 | 0.09473684210526316 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256+mac.checksum_ms` | 0.007017543859649123 | 0.021052631578947368 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256+mac.compress_ms` | 17.810526315789474 | 18.294736842105262 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256+mac.manifest_upload_ms` | 0.0 | 10.466666666666667 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256+mac.object_upload_ms` | 0.0 | 12.624561403508771 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256+mac.state_write_ms` | 1.0070175438596491 | 1.0140350877192983 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256+mac.verify_ms` | 0.4421052631578947 | 0.12631578947368421 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256-nocomp.checksum_ms` | 5.947368421052632 | 5.978947368421053 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256-nocomp.compress_ms` | 8.891228070175439 | 8.48421052631579 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256-nocomp.manifest_upload_ms` | 0.0 | 10.280701754385966 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256-nocomp.object_upload_ms` | 0.0 | 19.519298245614035 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|sha256-nocomp.verify_ms` | 7.094736842105263 | 5.982456140350878 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|size-only.compress_ms` | 17.659649122807018 | 18.442105263157895 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|size-only.manifest_upload_ms` | 0.0 | 7.2 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|size-only.object_upload_ms` | 0.0 | 10.228070175438596 |
| changed | `rq3.stage_mean_ms_per_batch.hdfs|size-only.state_write_ms` | 0.9649122807017544 | 1.0315789473684212 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|blake3.compress_ms` | 12.6 | 13.066666666666666 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|blake3.manifest_upload_ms` | 0.0 | 10.6 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|blake3.object_upload_ms` | 0.0 | 14.333333333333334 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|blake3.state_write_ms` | 0.9333333333333333 | 1.0 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|blake3.verify_ms` | 0.0 | 0.06666666666666667 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256.compress_ms` | 12.733333333333333 | 13.266666666666667 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256.manifest_upload_ms` | 0.0 | 8.733333333333333 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256.object_upload_ms` | 0.0 | 12.866666666666667 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256.state_write_ms` | 0.9333333333333333 | 1.0 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256+mac.compress_ms` | 12.866666666666667 | 13.333333333333334 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256+mac.manifest_upload_ms` | 0.0 | 8.466666666666667 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256+mac.object_upload_ms` | 0.0 | 12.8 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256+mac.state_write_ms` | 0.9333333333333333 | 1.0 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256-nocomp.checksum_ms` | 5.2 | 5.333333333333333 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256-nocomp.compress_ms` | 7.6 | 8.066666666666666 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256-nocomp.manifest_upload_ms` | 0.0 | 10.533333333333333 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256-nocomp.object_upload_ms` | 0.0 | 20.333333333333332 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256-nocomp.state_write_ms` | 1.0 | 1.2 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|sha256-nocomp.verify_ms` | 6.2 | 5.666666666666667 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|size-only.compress_ms` | 12.866666666666667 | 13.0 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|size-only.manifest_upload_ms` | 0.0 | 7.333333333333333 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|size-only.object_upload_ms` | 0.0 | 15.8 |
| changed | `rq3.stage_mean_ms_per_batch.ssh|size-only.state_write_ms` | 0.8666666666666667 | 1.0 |
| changed | `rq3.throughput_drop_vs_size_only_ci.bgl.blake3` | [6.6, 2.3, 9.0] | [10.7, 3.7, 16.7] |
| changed | `rq3.throughput_drop_vs_size_only_ci.bgl.sha256` | [5.4, 1.2, 6.8] | [10.0, 1.6, 14.8] |
| changed | `rq3.throughput_drop_vs_size_only_ci.bgl.sha256+mac` | [4.4, 0.6, 5.6] | [11.1, 3.8, 16.5] |
| changed | `rq3.throughput_drop_vs_size_only_ci.bgl.sha256-nocomp` | [7.5, 4.2, 9.3] | [18.2, 11.5, 23.4] |
| changed | `rq3.throughput_drop_vs_size_only_ci.hdfs.blake3` | [6.0, 3.0, 7.9] | [9.2, 6.1, 11.0] |
| changed | `rq3.throughput_drop_vs_size_only_ci.hdfs.sha256` | [4.3, 1.7, 5.8] | [10.2, 6.3, 13.3] |
| changed | `rq3.throughput_drop_vs_size_only_ci.hdfs.sha256+mac` | [4.4, 2.3, 6.0] | [8.2, 4.7, 10.8] |
| changed | `rq3.throughput_drop_vs_size_only_ci.hdfs.sha256-nocomp` | [11.3, 9.4, 13.3] | [19.6, 16.4, 22.5] |
| changed | `rq3.throughput_drop_vs_size_only_ci.ssh.blake3` | [5.7, 2.6, 8.5] | [8.0, 3.7, 13.0] |
| changed | `rq3.throughput_drop_vs_size_only_ci.ssh.sha256` | [5.0, 2.3, 8.7] | [9.4, 5.0, 14.4] |
| changed | `rq3.throughput_drop_vs_size_only_ci.ssh.sha256+mac` | [4.7, -0.3, 8.9] | [6.9, 2.8, 13.4] |
| changed | `rq3.throughput_drop_vs_size_only_ci.ssh.sha256-nocomp` | [13.7, 11.4, 17.6] | [23.5, 21.5, 33.0] |
| changed | `rq3.throughput_drop_vs_size_only_pct.bgl.blake3` | 6.6 | 10.7 |
| changed | `rq3.throughput_drop_vs_size_only_pct.bgl.sha256` | 5.4 | 10.0 |
| changed | `rq3.throughput_drop_vs_size_only_pct.bgl.sha256+mac` | 4.4 | 11.1 |
| changed | `rq3.throughput_drop_vs_size_only_pct.bgl.sha256-nocomp` | 7.5 | 18.2 |
| changed | `rq3.throughput_drop_vs_size_only_pct.hdfs.blake3` | 6.0 | 9.2 |
| changed | `rq3.throughput_drop_vs_size_only_pct.hdfs.sha256` | 4.3 | 10.2 |
| changed | `rq3.throughput_drop_vs_size_only_pct.hdfs.sha256+mac` | 4.4 | 8.2 |
| changed | `rq3.throughput_drop_vs_size_only_pct.hdfs.sha256-nocomp` | 11.3 | 19.6 |
| changed | `rq3.throughput_drop_vs_size_only_pct.ssh.blake3` | 5.7 | 8.0 |
| changed | `rq3.throughput_drop_vs_size_only_pct.ssh.sha256` | 5.0 | 9.4 |
| changed | `rq3.throughput_drop_vs_size_only_pct.ssh.sha256+mac` | 4.7 | 6.9 |
| changed | `rq3.throughput_drop_vs_size_only_pct.ssh.sha256-nocomp` | 13.7 | 23.5 |
| changed | `rq3.throughput_mib_s.bgl|blake3` | [332.69, 324.475, 341.82] | [185.01999999999998, 176.54, 192.65] |
| changed | `rq3.throughput_mib_s.bgl|sha256` | [337.155, 332.295, 343.65] | [186.39499999999998, 182.10500000000002, 196.05] |
| changed | `rq3.throughput_mib_s.bgl|sha256+mac` | [340.69500000000005, 336.195, 346.35] | [184.14, 177.45, 193.39] |
| changed | `rq3.throughput_mib_s.bgl|sha256-batch1` | [158.9, 148.48, 159.69] | [36.72, 36.31, 37.22] |
| changed | `rq3.throughput_mib_s.bgl|sha256-batch16` | [345.64, 339.38, 352.54] | [229.1, 226.99, 245.17] |
| changed | `rq3.throughput_mib_s.bgl|sha256-batch4` | [277.3, 265.24, 287.98] | [111.04, 110.22, 115.09] |
| changed | `rq3.throughput_mib_s.bgl|sha256-batch64` | [362.43, 349.28, 367.07] | [310.57, 302.15, 324.35] |
| changed | `rq3.throughput_mib_s.bgl|sha256-nocomp` | [329.485, 323.04999999999995, 331.47] | [169.51, 162.475, 175.73] |
| changed | `rq3.throughput_mib_s.bgl|size-only` | [356.25, 343.95, 358.595] | [207.14, 191.625, 216.26] |
| changed | `rq3.throughput_mib_s.hdfs|blake3` | [339.905, 333.31, 346.33] | [188.45, 185.71, 190.54000000000002] |
| changed | `rq3.throughput_mib_s.hdfs|sha256` | [345.865, 339.215, 351.51] | [186.24, 180.695, 189.695] |
| changed | `rq3.throughput_mib_s.hdfs|sha256+mac` | [345.57, 339.855, 349.94] | [190.42000000000002, 185.79, 193.015] |
| changed | `rq3.throughput_mib_s.hdfs|sha256-batch1` | [155.52, 152.92, 157.97] | [36.51, 36.21, 36.84] |
| changed | `rq3.throughput_mib_s.hdfs|sha256-batch16` | [347.85, 337.88, 354.24] | [228.59, 226.14, 234.93] |
| changed | `rq3.throughput_mib_s.hdfs|sha256-batch4` | [281.26, 270.52, 282.79] | [108.96, 106.63, 111.77] |
| changed | `rq3.throughput_mib_s.hdfs|sha256-batch64` | [362.36, 350.1, 366.2] | [311.31, 307.65, 320.15] |
| changed | `rq3.throughput_mib_s.hdfs|sha256-nocomp` | [320.46500000000003, 313.29, 325.0] | [166.765, 160.66, 170.485] |
| changed | `rq3.throughput_mib_s.hdfs|size-only` | [361.47, 356.32, 363.38] | [207.45999999999998, 201.1, 210.04000000000002] |
| changed | `rq3.throughput_mib_s.ssh|blake3` | [296.41, 292.49, 303.57] | [182.97500000000002, 178.8, 187.79500000000002] |
| changed | `rq3.throughput_mib_s.ssh|sha256` | [298.39, 290.53, 303.5] | [180.09, 176.05, 186.70499999999998] |
| changed | `rq3.throughput_mib_s.ssh|sha256+mac` | [299.6, 286.46500000000003, 314.7] | [185.195, 176.29, 189.98] |
| changed | `rq3.throughput_mib_s.ssh|sha256-nocomp` | [271.30999999999995, 262.01, 274.65999999999997] | [152.135, 134.95499999999998, 153.63] |
| changed | `rq3.throughput_mib_s.ssh|size-only` | [314.255, 306.71, 323.735] | [198.82, 189.09, 208.315] |
| changed | `rq4.baseline_sources.vtop` | "rq4-baselines/results/20261121-v0102-rq4" | "rq4-baselines/results/20261122-v0104-rq4" |
| changed | `rq4.cells.corrupt|vtop.elapsed_s_median` | 18.515 | 9.115 |
| changed | `rq4.cells.kill|vtop.elapsed_s_median` | 26.69 | 36.06 |
| changed | `rq4.cells.lost|vtop.elapsed_s_median` | 19.285 | 9.935 |
| changed | `rq4.vtop_elapsed_s_max` | 41.34 | 40.5 |
| changed | `rq5_cost.t4g.large.cells.hdfs.blake3-readback.allowance_exceeded_median` | 92.0 | 112.5 |
| changed | `rq5_cost.t4g.large.cells.hdfs.blake3-readback.change_vs_size_only_pct` | [-21.1055, -22.1919, -19.2341] | [-18.9477, -21.1638, -17.6825] |
| changed | `rq5_cost.t4g.large.cells.hdfs.blake3-readback.cpu_s_per_gib` | [11.0578, 10.984, 11.1275] | [11.3872, 11.2604, 11.4735] |
| changed | `rq5_cost.t4g.large.cells.hdfs.blake3-readback.cpu_steal_pct_max` | 0.141 | 1.109 |
| changed | `rq5_cost.t4g.large.cells.hdfs.blake3-readback.diff_vs_size_only_mib_s` | [-10.3855, -10.967, -9.4165] | [-8.911, -10.112, -8.312] |
| changed | `rq5_cost.t4g.large.cells.hdfs.blake3-readback.per_batch_total_ms_median` | 316.1263 | 320.9368 |
| changed | `rq5_cost.t4g.large.cells.hdfs.blake3-readback.throughput_mib_s` | [38.822, 38.5025, 39.568] | [38.1185, 37.443, 38.695] |
| changed | `rq5_cost.t4g.large.cells.hdfs.sha256-store.allowance_exceeded_median` | 74.5 | 28.0 |
| changed | `rq5_cost.t4g.large.cells.hdfs.sha256-store.change_vs_size_only_pct` | [-5.0673, -7.0607, -2.6176] | [-3.8774, -6.3623, -1.9337] |
| changed | `rq5_cost.t4g.large.cells.hdfs.sha256-store.cpu_s_per_gib` | [10.1903, 10.108, 10.4798] | [10.6005, 10.566, 10.8623] |
| changed | `rq5_cost.t4g.large.cells.hdfs.sha256-store.cpu_steal_pct_max` | 0.314 | 1.211 |
| changed | `rq5_cost.t4g.large.cells.hdfs.sha256-store.diff_vs_size_only_mib_s` | [-2.4935, -3.497, -1.285] | [-1.8235, -3.0395, -0.9785] |
| changed | `rq5_cost.t4g.large.cells.hdfs.sha256-store.per_batch_total_ms_median` | 251.7158 | 254.9842 |
| changed | `rq5_cost.t4g.large.cells.hdfs.sha256-store.throughput_mib_s` | [46.714, 45.8395, 47.805] | [45.206, 44.5205, 46.166] |
| changed | `rq5_cost.t4g.large.cells.hdfs.size-only.allowance_exceeded_median` | 75.5 | 40.0 |
| changed | `rq5_cost.t4g.large.cells.hdfs.size-only.cpu_s_per_gib` | [10.1225, 10.0058, 10.3166] | [10.5411, 10.3887, 10.7618] |
| changed | `rq5_cost.t4g.large.cells.hdfs.size-only.cpu_steal_pct_max` | 0.238 | 1.106 |
| changed | `rq5_cost.t4g.large.cells.hdfs.size-only.per_batch_total_ms_median` | 233.2737 | 238.7737 |
| changed | `rq5_cost.t4g.large.cells.hdfs.size-only.throughput_mib_s` | [49.2075, 48.618, 49.642] | [47.0295, 46.801, 48.0535] |
| changed | `rq5_cost.t4g.large.run_id` | "20261121T012945Z-i-072d88074e58c7845" | "20261122T041424Z-i-03c2592cd984f1276" |
| changed | `rq5_cost.t4g.large.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_cost.t4g.medium.cells.bgl.blake3-readback.allowance_exceeded_median` | 23.5 | 19.0 |
| changed | `rq5_cost.t4g.medium.cells.bgl.blake3-readback.change_vs_size_only_pct` | [-18.6665, -30.9999, -11.4451] | [-19.2519, -22.2008, -16.9691] |
| changed | `rq5_cost.t4g.medium.cells.bgl.blake3-readback.cpu_s_per_gib` | [10.8997, 10.7752, 11.0824] | [10.6002, 10.5169, 10.8477] |
| changed | `rq5_cost.t4g.medium.cells.bgl.blake3-readback.cpu_steal_pct_max` | 0.609 | 0.157 |
| changed | `rq5_cost.t4g.medium.cells.bgl.blake3-readback.diff_vs_size_only_mib_s` | [-8.718, -14.236, -4.92] | [-9.5675, -11.1551, -8.269] |
| changed | `rq5_cost.t4g.medium.cells.bgl.blake3-readback.per_batch_total_ms_median` | 331.9333 | 310.1333 |
| changed | `rq5_cost.t4g.medium.cells.bgl.blake3-readback.throughput_mib_s` | [37.986, 32.468, 38.6775] | [40.129, 38.375, 40.858] |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.allowance_exceeded_median` | 22.5 | 2.0 |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.change_vs_size_only_pct` | [-4.5221, -13.0897, 6.4669] | [-3.1964, -5.1712, -0.3451] |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.ci_resolution` | "includes 0 (no resolved difference)" | "below 0 (resolved slowdown)" |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.cpu_s_per_gib` | [10.1956, 10.0514, 10.4107] | [10.0102, 9.9229, 10.0818] |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.cpu_steal_pct_max` | 0.604 | 0.182 |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.diff_vs_size_only_mib_s` | [-2.112, -6.161, 2.8667] | [-1.5885, -2.605, -0.2205] |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.per_batch_total_ms_median` | 266.0222 | 241.5667 |
| changed | `rq5_cost.t4g.medium.cells.bgl.sha256-store.throughput_mib_s` | [44.592, 40.953, 47.01] | [48.108, 47.583, 48.8815] |
| changed | `rq5_cost.t4g.medium.cells.bgl.size-only.allowance_exceeded_median` | 12.5 | 54.0 |
| changed | `rq5_cost.t4g.medium.cells.bgl.size-only.cpu_s_per_gib` | [10.2801, 10.0753, 10.3735] | [9.9142, 9.7208, 10.033] |
| changed | `rq5_cost.t4g.medium.cells.bgl.size-only.cpu_steal_pct_max` | 0.752 | 0.117 |
| changed | `rq5_cost.t4g.medium.cells.bgl.size-only.per_batch_total_ms_median` | 249.3 | 230.1111 |
| changed | `rq5_cost.t4g.medium.cells.bgl.size-only.throughput_mib_s` | [46.704, 42.988, 48.542] | [49.6965, 48.398, 50.471] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.blake3-readback.allowance_exceeded_median` | 82.0 | 78.0 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.blake3-readback.change_vs_size_only_pct` | [-19.7544, -32.5475, -16.4429] | [-17.805, -19.5936, -16.767] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.blake3-readback.cpu_s_per_gib` | [11.2069, 11.1012, 11.5103] | [10.9686, 10.8509, 11.1326] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.blake3-readback.cpu_steal_pct_max` | 2.252 | 0.11 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.blake3-readback.diff_vs_size_only_mib_s` | [-9.267, -15.396, -7.5895] | [-8.7255, -9.81, -8.1219] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.blake3-readback.per_batch_total_ms_median` | 325.2474 | 305.7895 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.blake3-readback.throughput_mib_s` | [37.644, 31.691, 38.558] | [40.2805, 40.0175, 40.4645] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.sha256-store.allowance_exceeded_median` | 75.0 | 11.0 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.sha256-store.change_vs_size_only_pct` | [-11.8863, -23.1784, -5.2562] | [-3.3322, -5.5859, -1.9087] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.sha256-store.cpu_s_per_gib` | [10.6554, 10.4152, 10.7121] | [10.1501, 10.0949, 10.2537] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.sha256-store.cpu_steal_pct_max` | 0.732 | 0.09 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.sha256-store.diff_vs_size_only_mib_s` | [-5.576, -10.8375, -2.559] | [-1.633, -2.7575, -0.9255] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.sha256-store.per_batch_total_ms_median` | 291.9211 | 247.4211 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.sha256-store.throughput_mib_s` | [41.335, 36.173, 44.5255] | [47.373, 46.803, 47.809] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.size-only.allowance_exceeded_median` | 52.0 | 46.5 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.size-only.cpu_s_per_gib` | [10.5271, 10.2907, 10.6573] | [10.1568, 10.0806, 10.2654] |
| changed | `rq5_cost.t4g.medium.cells.hdfs.size-only.cpu_steal_pct_max` | 1.035 | 0.529 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.size-only.per_batch_total_ms_median` | 245.4421 | 232.3789 |
| changed | `rq5_cost.t4g.medium.cells.hdfs.size-only.throughput_mib_s` | [46.911, 45.2525, 48.097] | [49.006, 48.422, 50.019] |
| changed | `rq5_cost.t4g.medium.cells.ssh.blake3-readback.change_vs_size_only_pct` | [-14.2499, -20.0473, -5.4869] | [-16.2886, -19.9246, -13.7217] |
| changed | `rq5_cost.t4g.medium.cells.ssh.blake3-readback.cpu_s_per_gib` | [11.6884, 11.1444, 11.8054] | [11.2994, 10.5572, 11.537] |
| changed | `rq5_cost.t4g.medium.cells.ssh.blake3-readback.cpu_steal_pct_max` | 1.134 | 0.13 |
| changed | `rq5_cost.t4g.medium.cells.ssh.blake3-readback.diff_vs_size_only_mib_s` | [-5.912, -9.061, -2.237] | [-7.51, -9.3007, -6.169] |
| changed | `rq5_cost.t4g.medium.cells.ssh.blake3-readback.per_batch_total_ms_median` | 313.2 | 291.8 |
| changed | `rq5_cost.t4g.medium.cells.ssh.blake3-readback.throughput_mib_s` | [35.576, 34.754, 37.2155] | [38.596, 37.248, 39.6295] |
| changed | `rq5_cost.t4g.medium.cells.ssh.sha256-store.change_vs_size_only_pct` | [4.9279, -3.6746, 13.7342] | [-3.4985, -6.5603, 0.0918] |
| changed | `rq5_cost.t4g.medium.cells.ssh.sha256-store.cpu_s_per_gib` | [10.8299, 10.5052, 11.1166] | [10.8197, 10.2347, 10.9937] |
| changed | `rq5_cost.t4g.medium.cells.ssh.sha256-store.cpu_steal_pct_max` | 0.287 | 0.148 |
| changed | `rq5_cost.t4g.medium.cells.ssh.sha256-store.diff_vs_size_only_mib_s` | [2.0445, -1.6455, 5.5685] | [-1.613, -3.07, 0.0436] |
| changed | `rq5_cost.t4g.medium.cells.ssh.sha256-store.per_batch_total_ms_median` | 245.9 | 231.0 |
| changed | `rq5_cost.t4g.medium.cells.ssh.sha256-store.throughput_mib_s` | [43.5325, 41.3391, 44.8305] | [44.493, 43.447, 45.783] |
| changed | `rq5_cost.t4g.medium.cells.ssh.size-only.cpu_s_per_gib` | [10.9308, 10.7495, 11.0259] | [10.6383, 10.4146, 10.7327] |
| changed | `rq5_cost.t4g.medium.cells.ssh.size-only.cpu_steal_pct_max` | 0.277 | 0.148 |
| changed | `rq5_cost.t4g.medium.cells.ssh.size-only.per_batch_total_ms_median` | 250.4 | 225.7 |
| changed | `rq5_cost.t4g.medium.cells.ssh.size-only.throughput_mib_s` | [41.488, 37.9475, 44.8835] | [46.106, 44.822, 47.171] |
| changed | `rq5_cost.t4g.medium.run_id` | "20261121T002644Z-i-07a97d2f7a612888e" | "20261122T021852Z-i-0d197e09de6af68de" |
| changed | `rq5_cost.t4g.medium.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_perf.claims.confluent_over_vtop_size_only.ap-southeast-2` | 3.57 | 2.98 |
| changed | `rq5_perf.claims.confluent_over_vtop_size_only.us-east-1` | 1.93 | 1.54 |
| changed | `rq5_perf.claims.confluent_over_vtop_store.ap-southeast-2` | 4.11 | 3.65 |
| changed | `rq5_perf.claims.confluent_over_vtop_store.us-east-1` | 1.91 | 1.64 |
| changed | `rq5_perf.claims.cpu_s_per_gib.ap-southeast-2.confluent` | 57.822 | 55.788 |
| changed | `rq5_perf.claims.cpu_s_per_gib.ap-southeast-2.fluentbit` | 26.225 | 26.221 |
| changed | `rq5_perf.claims.cpu_s_per_gib.ap-southeast-2.vector` | 207.771 | 207.333 |
| changed | `rq5_perf.claims.cpu_s_per_gib.ap-southeast-2.vtop-blake3-readback` | 22.181 | 24.46 |
| changed | `rq5_perf.claims.cpu_s_per_gib.ap-southeast-2.vtop-sha256-store` | 21.519 | 23.953 |
| changed | `rq5_perf.claims.cpu_s_per_gib.ap-southeast-2.vtop-size-only` | 21.765 | 23.869 |
| changed | `rq5_perf.claims.cpu_s_per_gib.us-east-1.confluent` | 54.204 | 53.101 |
| changed | `rq5_perf.claims.cpu_s_per_gib.us-east-1.fluentbit` | 26.495 | 26.295 |
| changed | `rq5_perf.claims.cpu_s_per_gib.us-east-1.vector` | 202.096 | 203.792 |
| changed | `rq5_perf.claims.cpu_s_per_gib.us-east-1.vtop-blake3-readback` | 23.177 | 25.279 |
| changed | `rq5_perf.claims.cpu_s_per_gib.us-east-1.vtop-sha256-gzip6` | 47.602 | 49.813 |
| changed | `rq5_perf.claims.cpu_s_per_gib.us-east-1.vtop-sha256-store` | 22.01 | 23.961 |
| changed | `rq5_perf.claims.cpu_s_per_gib.us-east-1.vtop-size-only` | 22.139 | 24.556 |
| changed | `rq5_perf.claims.fault_sinks_rq4_comparable` | 28 | 26 |
| changed | `rq5_perf.claims.fault_sinks_unsafe_rq4_comparable` | 28 | 26 |
| changed | `rq5_perf.claims.fault_vtop_damage_before_verification_ms` | [41.6] | [6.3, 18.1] |
| changed | `rq5_perf.claims.fault_vtop_vs_verification.after_verification` | 9 | 8 |
| changed | `rq5_perf.claims.fault_vtop_vs_verification.before_verification` | 1 | 2 |
| changed | `rq5_perf.claims.kill_duplicate_records.ap-southeast-2.vector` | [240000, 240000] | [0, 0] |
| changed | `rq5_perf.claims.kill_duplicate_records.us-east-1.vector` | [240000, 240000, 120000] | [168529, 0, 240000] |
| changed | `rq5_perf.claims.kill_first_commit_s.ap-southeast-2.confluent` | 53.372 | 51.159 |
| changed | `rq5_perf.claims.kill_first_commit_s.ap-southeast-2.fluentbit` | 45.304 | 45.2955 |
| changed | `rq5_perf.claims.kill_first_commit_s.ap-southeast-2.vector` | 15.6135 | 15.518 |
| changed | `rq5_perf.claims.kill_first_commit_s.ap-southeast-2.vtop-sha256-store` | 3.871 | 5.624 |
| changed | `rq5_perf.claims.kill_first_commit_s.us-east-1.confluent` | 49.866 | 50.102 |
| changed | `rq5_perf.claims.kill_first_commit_s.us-east-1.fluentbit` | 45.293 | 45.282 |
| changed | `rq5_perf.claims.kill_first_commit_s.us-east-1.vector` | 10.521 | 10.735 |
| changed | `rq5_perf.claims.kill_first_commit_s.us-east-1.vtop-sha256-store` | 2.745 | 1.695 |
| changed | `rq5_perf.claims.live_p50_s.ap-southeast-2.confluent` | 4.1738 | 4.1792 |
| changed | `rq5_perf.claims.live_p50_s.ap-southeast-2.fluentbit` | 4.4609 | 4.5238 |
| changed | `rq5_perf.claims.live_p50_s.ap-southeast-2.vector` | 6.1427 | 6.1308 |
| changed | `rq5_perf.claims.live_p50_s.ap-southeast-2.vtop-blake3-readback` | 6.8517 | 6.7391 |
| changed | `rq5_perf.claims.live_p50_s.ap-southeast-2.vtop-sha256-store` | 4.7186 | 4.6957 |
| changed | `rq5_perf.claims.live_p50_s.ap-southeast-2.vtop-size-only` | 4.3853 | 4.4506 |
| changed | `rq5_perf.claims.live_p50_s.us-east-1.confluent` | 2.4875 | 2.6761 |
| changed | `rq5_perf.claims.live_p50_s.us-east-1.fluentbit` | 3.154 | 3.1782 |
| changed | `rq5_perf.claims.live_p50_s.us-east-1.vector` | 5.1525 | 5.1553 |
| changed | `rq5_perf.claims.live_p50_s.us-east-1.vtop-blake3-readback` | 2.7605 | 2.8127 |
| changed | `rq5_perf.claims.live_p50_s.us-east-1.vtop-sha256-store` | 2.7515 | 2.698 |
| changed | `rq5_perf.claims.live_p50_s.us-east-1.vtop-size-only` | 2.6975 | 2.7326 |
| changed | `rq5_perf.claims.peak_anon_mib.ap-southeast-2.confluent` | 952.8555 | 1129.6953 |
| changed | `rq5_perf.claims.peak_anon_mib.ap-southeast-2.fluentbit` | 140.8594 | 144.0547 |
| changed | `rq5_perf.claims.peak_anon_mib.ap-southeast-2.vector` | 567.4219 | 572.7656 |
| changed | `rq5_perf.claims.peak_anon_mib.ap-southeast-2.vtop-blake3-readback` | 223.6406 | 218.9023 |
| changed | `rq5_perf.claims.peak_anon_mib.ap-southeast-2.vtop-sha256-store` | 223.4023 | 228.1055 |
| changed | `rq5_perf.claims.peak_anon_mib.ap-southeast-2.vtop-size-only` | 229.6758 | 235.332 |
| changed | `rq5_perf.claims.peak_anon_mib.us-east-1.confluent` | 1205.0156 | 1012.7266 |
| changed | `rq5_perf.claims.peak_anon_mib.us-east-1.fluentbit` | 123.6719 | 131.9609 |
| changed | `rq5_perf.claims.peak_anon_mib.us-east-1.vector` | 562.0508 | 559.7969 |
| changed | `rq5_perf.claims.peak_anon_mib.us-east-1.vtop-blake3-readback` | 218.2461 | 224.3555 |
| changed | `rq5_perf.claims.peak_anon_mib.us-east-1.vtop-sha256-gzip6` | 215.2578 | 206.6719 |
| changed | `rq5_perf.claims.peak_anon_mib.us-east-1.vtop-sha256-store` | 233.8867 | 220.2734 |
| changed | `rq5_perf.claims.peak_anon_mib.us-east-1.vtop-size-only` | 229.1055 | 227.7422 |
| changed | `rq5_perf.claims.rtt_ms.ap-southeast-2` | 196.2 | 196.43 |
| changed | `rq5_perf.claims.rtt_ms.us-east-1` | 0.63 | 0.45 |
| changed | `rq5_perf.claims.size_only_vs_store_pct.ap-southeast-2` | [15.2957, 9.2563, 24.3163] | [22.7362, 8.8744, 39.0835] |
| changed | `rq5_perf.claims.size_only_vs_store_pct.us-east-1` | [-1.1477, -7.5848, 10.0026] | [6.3537, -2.2371, 23.1506] |
| changed | `rq5_perf.claims.vector_over_vtop_store.ap-southeast-2` | 1.53 | 1.32 |
| changed | `rq5_perf.claims.vector_over_vtop_store.us-east-1` | 0.49 | 0.41 |
| changed | `rq5_perf.claims.vtop_store_cores.ap-southeast-2` | 0.127 | 0.158 |
| changed | `rq5_perf.claims.vtop_store_cores.us-east-1` | 0.427 | 0.54 |
| changed | `rq5_perf.distance_effect.confluent.done_far_vs_near_pct` | [-33.6117, -40.9331, -27.1794] | [-31.5088, -34.4733, -28.6957] |
| changed | `rq5_perf.distance_effect.confluent.live_p50_far_minus_near_s` | [1.6863, 1.6284, 1.7107] | [1.5031, 1.4803, 1.5377] |
| changed | `rq5_perf.distance_effect.fluentbit.done_far_vs_near_pct` | [-0.0621, -0.1655, 0.2276] | [0.0621, -0.3502, 0.0827] |
| changed | `rq5_perf.distance_effect.fluentbit.live_p50_far_minus_near_s` | [1.3069, 1.1423, 1.4271] | [1.3456, 1.3213, 1.6469] |
| changed | `rq5_perf.distance_effect.vector.done_far_vs_near_pct` | [-3.8602, -6.1738, -1.3215] | [-1.3811, -6.7305, 2.7573] |
| changed | `rq5_perf.distance_effect.vector.live_p50_far_minus_near_s` | [0.9902, 0.9395, 1.0278] | [0.9755, 0.9061, 1.0243] |
| changed | `rq5_perf.distance_effect.vtop-blake3-readback.done_far_vs_near_pct` | [-73.5486, -75.4205, -72.2543] | [-76.8189, -79.9414, -73.3223] |
| changed | `rq5_perf.distance_effect.vtop-blake3-readback.live_p50_far_minus_near_s` | [4.0912, 3.9155, 4.3238] | [3.9264, 3.8374, 3.9795] |
| changed | `rq5_perf.distance_effect.vtop-sha256-store.done_far_vs_near_pct` | [-69.1201, -70.4884, -68.5576] | [-69.2927, -71.7666, -66.0555] |
| changed | `rq5_perf.distance_effect.vtop-sha256-store.live_p50_far_minus_near_s` | [1.9671, 1.9088, 2.0417] | [1.9977, 1.8351, 2.0548] |
| changed | `rq5_perf.distance_effect.vtop-size-only.done_far_vs_near_pct` | [-63.9835, -67.6342, -61.2563] | [-64.5626, -68.51, -61.552] |
| changed | `rq5_perf.distance_effect.vtop-size-only.live_p50_far_minus_near_s` | [1.6878, 1.5624, 1.7927] | [1.718, 1.6614, 1.7392] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.confluent.first_commit_after_s` | [53.372, 50.894, 55.85] | [51.159, 50.378, 51.94] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.confluent.first_object_after_s` | [53.9355, 51.368, 56.503] | [51.4625, 50.792, 52.133] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.confluent.ready_s` | [46.287, 43.18, 49.394] | [43.909, 43.686, 44.132] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.confluent.resteady_s` | [64.7735, 57.405, 72.142] | [62.6705, 61.705, 63.636] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.fluentbit.first_commit_after_s` | [45.304, 45.302, 45.306] | [45.2955, 45.294, 45.297] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.fluentbit.first_object_after_s` | [3.5425, 3.139, 3.946] | [3.7535, 3.572, 3.935] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.fluentbit.ready_s` | [0.268, 0.247, 0.289] | [0.268, 0.258, 0.278] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.fluentbit.resteady_s` | [53.809, 53.807, 53.811] | [53.7995, 53.798, 53.801] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vector.duplicate_records` | [240000, 240000] | [0, 0] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vector.first_commit_after_s` | [15.6135, 15.495, 15.732] | [15.518, 15.5, 15.536] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vector.first_object_after_s` | [7.297, 0.508, 14.086] | [14.4525, 14.242, 14.663] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vector.ready_s` | [0.331, 0.299, 0.363] | [0.314, 0.307, 0.321] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vector.resteady_s` | [25.62, 25.5, 25.74] | [28.0245, 25.506, 30.543] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vtop-sha256-store.first_commit_after_s` | [3.871, 1.243, 6.499] | [5.624, 4.996, 6.252] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vtop-sha256-store.first_object_after_s` | [7.0415, 6.518, 7.565] | [6.6855, 5.653, 7.718] |
| changed | `rq5_perf.legs.ap-southeast-2.kill.vtop-sha256-store.resteady_s` | [13.751, 11.498, 16.004] | [11.7595, 11.254, 12.265] |
| changed | `rq5_perf.legs.ap-southeast-2.live.confluent.commit_s_p50` | [4.0293, 3.9674, 4.0913] | [4.0655, 4.0252, 4.1058] |
| changed | `rq5_perf.legs.ap-southeast-2.live.confluent.end_to_end_s_p100` | [6.3915, 6.3289, 6.4541] | [6.4259, 6.3554, 6.4964] |
| changed | `rq5_perf.legs.ap-southeast-2.live.confluent.end_to_end_s_p50` | [4.1738, 4.1578, 4.1898] | [4.1792, 4.1629, 4.1955] |
| changed | `rq5_perf.legs.ap-southeast-2.live.confluent.end_to_end_s_p90` | [5.7989, 5.7873, 5.8105] | [5.8199, 5.8161, 5.8237] |
| changed | `rq5_perf.legs.ap-southeast-2.live.confluent.end_to_end_s_p99` | [6.2149, 6.2132, 6.2167] | [6.2504, 6.223, 6.2777] |
| changed | `rq5_perf.legs.ap-southeast-2.live.confluent.object_visible_s_p50` | [4.1368, 4.1228, 4.1509] | [4.1188, 4.0887, 4.1488] |
| changed | `rq5_perf.legs.ap-southeast-2.live.fluentbit.commit_s_p50` | [0.3874, 0.2984, 0.4765] | [0.4265, 0.3727, 0.4804] |
| changed | `rq5_perf.legs.ap-southeast-2.live.fluentbit.end_to_end_s_p100` | [7.4045, 7.2881, 7.5209] | [7.513, 7.4909, 7.5351] |
| changed | `rq5_perf.legs.ap-southeast-2.live.fluentbit.end_to_end_s_p50` | [4.4609, 4.4407, 4.4811] | [4.5238, 4.5084, 4.5392] |
| changed | `rq5_perf.legs.ap-southeast-2.live.fluentbit.end_to_end_s_p90` | [6.1798, 6.1644, 6.1952] | [6.3069, 6.2861, 6.3276] |
| changed | `rq5_perf.legs.ap-southeast-2.live.fluentbit.end_to_end_s_p99` | [7.0182, 6.9136, 7.1227] | [7.1132, 7.091, 7.1354] |
| changed | `rq5_perf.legs.ap-southeast-2.live.fluentbit.object_visible_s_p50` | [4.4609, 4.4407, 4.4811] | [4.5238, 4.5084, 4.5392] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vector.commit_s_p50` | [6.1414, 6.1071, 6.1758] | [6.1308, 6.1, 6.1617] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vector.end_to_end_s_p100` | [10.1452, 10.1099, 10.1805] | [10.1389, 10.1127, 10.1651] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vector.end_to_end_s_p50` | [6.1427, 6.1071, 6.1783] | [6.1308, 6.1, 6.1617] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vector.end_to_end_s_p90` | [8.6454, 8.6096, 8.6811] | [8.6341, 8.6037, 8.6645] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vector.end_to_end_s_p99` | [9.9363, 9.9061, 9.9665] | [9.9302, 9.9009, 9.9594] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vector.object_visible_s_p50` | [3.9123, 3.8911, 3.9336] | [3.8493, 3.811, 3.8875] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-blake3-readback.commit_s_p50` | [6.8517, 6.6868, 7.0166] | [6.7391, 6.737, 6.7412] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-blake3-readback.end_to_end_s_p100` | [9.5975, 8.5646, 10.6304] | [10.0063, 9.6071, 10.4055] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-blake3-readback.end_to_end_s_p50` | [6.8517, 6.6868, 7.0166] | [6.7391, 6.737, 6.7412] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-blake3-readback.end_to_end_s_p90` | [8.2652, 7.7661, 8.7642] | [8.1196, 7.8886, 8.3506] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-blake3-readback.end_to_end_s_p99` | [9.2549, 8.278, 10.2317] | [9.6072, 9.207, 10.0073] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-blake3-readback.object_visible_s_p50` | [5.8632, 5.6605, 6.0659] | [5.7598, 5.7197, 5.7999] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-sha256-store.commit_s_p50` | [4.7186, 4.675, 4.7622] | [4.6957, 4.6409, 4.7505] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-sha256-store.end_to_end_s_p100` | [6.9701, 6.902, 7.0381] | [7.001, 6.9929, 7.0092] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-sha256-store.end_to_end_s_p50` | [4.7186, 4.675, 4.7622] | [4.6957, 4.6409, 4.7505] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-sha256-store.end_to_end_s_p90` | [6.2074, 6.141, 6.2739] | [6.1312, 6.1086, 6.1539] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-sha256-store.end_to_end_s_p99` | [6.7393, 6.725, 6.7536] | [6.7561, 6.7452, 6.767] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-sha256-store.object_visible_s_p50` | [4.2153, 4.1322, 4.2983] | [4.1972, 4.1883, 4.206] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-size-only.commit_s_p50` | [4.3853, 4.3335, 4.437] | [4.4489, 4.4332, 4.4647] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-size-only.end_to_end_s_p100` | [6.5213, 6.4817, 6.5609] | [7.0202, 7.0028, 7.0376] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-size-only.end_to_end_s_p50` | [4.3853, 4.3335, 4.437] | [4.4506, 4.4332, 4.4681] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-size-only.end_to_end_s_p90` | [5.8416, 5.806, 5.8773] | [5.971, 5.9556, 5.9865] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-size-only.end_to_end_s_p99` | [6.3697, 6.3532, 6.3861] | [6.646, 6.6045, 6.6875] |
| changed | `rq5_perf.legs.ap-southeast-2.live.vtop-size-only.object_visible_s_p50` | [3.8771, 3.8088, 3.9454] | [3.9398, 3.8551, 4.0245] |
| changed | `rq5_perf.legs.ap-southeast-2.rtt_sink_ms_median.end` | 196.3 | 196.25 |
| changed | `rq5_perf.legs.ap-southeast-2.rtt_sink_ms_median.start` | 196.2 | 196.43 |
| changed | `rq5_perf.legs.ap-southeast-2.session` | "20261121T150129Z-apse2" | "20261122T032729Z-apse2" |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.archived_mib_s` | [24.855, 23.371, 25.183] | [26.646, 25.33, 26.69] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.committed_mib_s` | [24.56, 23.357, 24.971] | [25.956, 25.391, 26.375] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.cpu_cores_median` | 1.405 | 1.434 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.cpu_per_gib_vs_ref_pct` | [168.7021, 134.8463, 197.025] | [132.9061, 118.3838, 164.5097] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.cpu_s_per_gib` | [57.822, 53.397, 62.301] | [55.788, 52.672, 60.131] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.done_mib_s` | [24.885, 23.371, 24.971] | [25.902, 25.33, 26.326] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.done_vs_ref_pct` | [311.0505, 279.5226, 313.9068] | [265.3315, 237.0144, 289.15] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.ena_allowance_exceeded_sink` | 0 | 3 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.peak_anon_mib_median` | 952.8555 | 1129.6953 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.sink_ec2_usd_per_gib` | 0.000969 | 0.000931 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.confluent.transfer_usd_per_gib` | 0.01415 | 0.014174 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.archived_mib_s` | [4.831, 4.826, 4.845] | [4.837, 4.829, 4.838] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.committed_mib_s` | [4.835, 4.832, 4.837] | [4.837, 4.832, 4.837] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.compression_ratio_median` | 8.5451 | 8.5462 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.cpu_per_gib_vs_ref_pct` | [21.869, 14.6809, 25.8641] | [9.4685, 8.4539, 15.7172] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.cpu_s_per_gib` | [26.225, 26.075, 26.4] | [26.221, 26.158, 26.306] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.done_mib_s` | [4.831, 4.826, 4.845] | [4.837, 4.829, 4.838] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.done_vs_ref_pct` | [-20.2015, -21.6304, -19.6917] | [-31.7772, -35.7504, -28.4848] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.ena_allowance_exceeded_sink` | 7057 | 221 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.peak_anon_mib_median` | 140.8594 | 144.0547 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.raw_mib_per_object_median` | 17.0954 | 17.0914 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.sink_ec2_usd_per_gib` | 0.004993 | 0.004987 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.fluentbit.transfer_usd_per_gib` | 0.012583 | 0.01259 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.archived_mib_s` | [9.353, 9.283, 9.478] | [9.333, 9.142, 9.634] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.committed_mib_s` | [9.369, 9.227, 9.553] | [9.354, 9.217, 9.615] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.compression_ratio_median` | 7.6429 | 7.7648 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.cpu_cores_median` | 1.877 | 1.894 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.cpu_per_gib_vs_ref_pct` | [865.5235, 791.2741, 899.4327] | [765.5826, 739.2263, 825.7467] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.cpu_s_per_gib` | [207.771, 202.649, 209.631] | [207.333, 202.413, 210.45] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.done_mib_s` | [9.24, 9.227, 9.483] | [9.354, 9.217, 9.615] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.done_vs_ref_pct` | [52.6264, 49.8376, 57.1855] | [31.9323, 22.6317, 42.1286] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.ena_allowance_exceeded_sink` | 259 | 24 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.peak_anon_mib_median` | 567.4219 | 572.7656 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.raw_mib_per_object_median` | 16.2524 | 16.2598 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.sink_ec2_usd_per_gib` | 0.00261 | 0.002579 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vector.transfer_usd_per_gib` | 0.014038 | 0.013762 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.archived_mib_s` | [4.959, 4.929, 5.097] | [5.268, 5.076, 5.588] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.committed_mib_s` | [4.998, 4.993, 5.088] | [5.273, 5.074, 5.629] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.compression_ratio_median` | 7.9299 | 7.9278 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.cpu_cores_median` | 0.109 | 0.126 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.cpu_per_gib_vs_ref_pct` | [3.0764, -3.2722, 8.0191] | [2.1166, -0.3151, 7.9708] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.cpu_s_per_gib` | [22.181, 21.993, 22.657] | [24.46, 24.043, 24.545] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.data_objects_per_gib` | 78.4025 | 79.0963 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.done_mib_s` | [4.998, 4.993, 5.088] | [5.273, 5.074, 5.629] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.done_vs_ref_pct` | [-17.443, -18.9185, -15.6638] | [-25.6276, -32.4907, -16.7923] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.done_vs_size_only_pct` | [-28.3954, -33.4267, -24.3757] | [-39.4047, -46.0729, -31.211] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.ena_allowance_exceeded_sink` | 14501 | 671 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.peak_anon_mib_median` | 223.6406 | 218.9023 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.raw_mib_per_object_median` | 13.6703 | 13.6535 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.request_usd_per_gib_est` | 0.000941 | 0.000949 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.sink_ec2_usd_per_gib` | 0.004826 | 0.004574 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-blake3-readback.transfer_usd_per_gib` | 0.016767 | 0.016532 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.archived_mib_s` | [6.075, 6.049, 6.165] | [7.088, 6.77, 7.511] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.committed_mib_s` | [6.054, 6.033, 6.158] | [7.09, 6.765, 7.516] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.compression_ratio_median` | 7.9293 | 7.9289 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.cpu_cores_median` | 0.127 | 0.158 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.cpu_s_per_gib` | [21.519, 20.975, 22.737] | [23.953, 22.733, 24.119] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.data_objects_per_gib` | 78.4025 | 77.0148 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.done_mib_s` | [6.054, 6.033, 6.158] | [7.09, 6.765, 7.516] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.done_vs_size_only_pct` | [-13.2665, -19.56, -8.4721] | [-18.5245, -28.1008, -8.151] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.ena_allowance_exceeded_sink` | 13727 | 918 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.peak_anon_mib_median` | 223.4023 | 228.1055 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.raw_mib_per_object_median` | 13.5304 | 13.8085 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.request_usd_per_gib_est` | 0.000878 | 0.000863 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.sink_ec2_usd_per_gib` | 0.003984 | 0.003402 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-sha256-store.transfer_usd_per_gib` | 0.013666 | 0.013793 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.archived_mib_s` | [6.975, 6.775, 7.428] | [8.823, 8.179, 9.313] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.committed_mib_s` | [6.98, 6.728, 7.497] | [8.702, 8.178, 9.409] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.compression_ratio_median` | 7.9289 | 7.9278 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.cpu_cores_median` | 0.148 | 0.212 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.cpu_per_gib_vs_ref_pct` | [1.1432, -8.959, 4.3433] | [-0.3507, -4.8427, 9.528] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.cpu_s_per_gib` | [21.765, 20.7, 21.886] | [23.869, 22.951, 24.899] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.data_objects_per_gib` | 75.6272 | 77.0148 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.done_mib_s` | [6.98, 6.728, 7.5] | [8.702, 8.183, 9.409] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.done_vs_ref_pct` | [15.2957, 9.2563, 24.3163] | [22.7362, 8.8744, 39.0835] |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.ena_allowance_exceeded_sink` | 16046 | 1278 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.peak_anon_mib_median` | 229.6758 | 235.332 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.raw_mib_per_object_median` | 13.8114 | 13.7219 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.request_usd_per_gib_est` | 0.000817 | 0.000832 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.sink_ec2_usd_per_gib` | 0.003456 | 0.002772 |
| changed | `rq5_perf.legs.ap-southeast-2.tput.vtop-size-only.transfer_usd_per_gib` | 0.013754 | 0.013751 |
| changed | `rq5_perf.legs.ap-southeast-2.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_perf.legs.us-east-1.faults.corrupt/confluent.classes.ambiguous_within_one_poll` | 1 | 2 |
| removed | `rq5_perf.legs.us-east-1.faults.corrupt/confluent.classes.damage_after_commit` | 1 | "\u2014" |
| changed | `rq5_perf.legs.us-east-1.faults.corrupt/vtop-blake3-readback.classes.ambiguous_within_one_poll` | 3 | 4 |
| changed | `rq5_perf.legs.us-east-1.faults.corrupt/vtop-blake3-readback.classes.damage_after_commit` | 2 | 1 |
| changed | `rq5_perf.legs.us-east-1.faults.corrupt/vtop-blake3-readback.damage_before_verification_ms` | [] | [6.3] |
| changed | `rq5_perf.legs.us-east-1.faults.corrupt/vtop-blake3-readback.vtop_vs_verification.after_verification` | 5 | 4 |
| added | `rq5_perf.legs.us-east-1.faults.corrupt/vtop-blake3-readback.vtop_vs_verification.before_verification` | "\u2014" | 1 |
| added | `rq5_perf.legs.us-east-1.faults.lost/confluent.classes.ambiguous_within_one_poll` | "\u2014" | 2 |
| changed | `rq5_perf.legs.us-east-1.faults.lost/confluent.classes.damage_before_commit` | 5 | 3 |
| changed | `rq5_perf.legs.us-east-1.faults.lost/confluent.rq4_comparable_trials` | 5 | 3 |
| changed | `rq5_perf.legs.us-east-1.faults.lost/confluent.unsafe_trials_rq4_comparable` | 5 | 3 |
| changed | `rq5_perf.legs.us-east-1.faults.lost/vtop-blake3-readback.damage_before_verification_ms` | [41.6] | [18.1] |
| changed | `rq5_perf.legs.us-east-1.kill.confluent.first_commit_after_s` | [49.866, 49.409, 50.283] | [50.102, 49.884, 50.863] |
| changed | `rq5_perf.legs.us-east-1.kill.confluent.first_object_after_s` | [0.494, 0.355, 0.83] | [50.821, 49.849, 51.085] |
| changed | `rq5_perf.legs.us-east-1.kill.confluent.ready_s` | [44.726, 44.233, 45.261] | [45.252, 45.245, 45.773] |
| changed | `rq5_perf.legs.us-east-1.kill.confluent.resteady_s` | [57.87, 57.663, 58.538] | [58.356, 58.139, 59.12] |
| changed | `rq5_perf.legs.us-east-1.kill.fluentbit.first_commit_after_s` | [45.293, 45.268, 45.317] | [45.282, 45.273, 45.513] |
| changed | `rq5_perf.legs.us-east-1.kill.fluentbit.first_object_after_s` | [1.01, 0.965, 1.23] | [0.976, 0.708, 1.194] |
| changed | `rq5_perf.legs.us-east-1.kill.fluentbit.ready_s` | [0.238, 0.227, 0.251] | [0.298, 0.27, 0.322] |
| changed | `rq5_perf.legs.us-east-1.kill.fluentbit.resteady_s` | [53.809, 53.773, 53.821] | [53.787, 53.541, 54.025] |
| changed | `rq5_perf.legs.us-east-1.kill.vector.duplicate_records` | [240000, 240000, 120000] | [168529, 0, 240000] |
| changed | `rq5_perf.legs.us-east-1.kill.vector.first_commit_after_s` | [10.521, 10.504, 15.746] | [10.735, 10.48, 15.472] |
| changed | `rq5_perf.legs.us-east-1.kill.vector.first_object_after_s` | [9.756, 9.742, 11.734] | [9.712, 0.452, 9.726] |
| changed | `rq5_perf.legs.us-east-1.kill.vector.ready_s` | [0.294, 0.263, 0.43] | [0.325, 0.307, 0.354] |
| changed | `rq5_perf.legs.us-east-1.kill.vector.resteady_s` | [25.529, 20.509, 25.751] | [25.478, 20.487, 25.743] |
| changed | `rq5_perf.legs.us-east-1.kill.vtop-sha256-store.first_commit_after_s` | [2.745, 1.494, 3.969] | [1.695, 1.493, 1.993] |
| changed | `rq5_perf.legs.us-east-1.kill.vtop-sha256-store.first_object_after_s` | [0.486, 0.242, 3.987] | [0.731, 0.206, 1.719] |
| changed | `rq5_perf.legs.us-east-1.kill.vtop-sha256-store.resteady_s` | [11.255, 10.722, 11.769] | [13.502, 10.757, 21.992] |
| changed | `rq5_perf.legs.us-east-1.live.confluent.commit_s_p50` | [2.4875, 2.4791, 2.5294] | [2.6761, 2.6578, 2.6819] |
| changed | `rq5_perf.legs.us-east-1.live.confluent.end_to_end_s_p100` | [4.4973, 4.4896, 4.5428] | [4.6866, 4.6772, 4.6972] |
| changed | `rq5_perf.legs.us-east-1.live.confluent.end_to_end_s_p50` | [2.4875, 2.4791, 2.5294] | [2.6761, 2.6578, 2.6826] |
| changed | `rq5_perf.legs.us-east-1.live.confluent.end_to_end_s_p90` | [4.0891, 4.0804, 4.132] | [4.2784, 4.2614, 4.2849] |
| changed | `rq5_perf.legs.us-east-1.live.confluent.end_to_end_s_p99` | [4.4495, 4.4408, 4.492] | [4.6381, 4.6209, 4.6446] |
| changed | `rq5_perf.legs.us-east-1.live.confluent.object_visible_s_p50` | [2.2323, 2.226, 2.2774] | [2.4366, 2.4243, 2.5663] |
| changed | `rq5_perf.legs.us-east-1.live.fluentbit.commit_s_p50` | [0.3921, 0.2913, 0.4394] | [0.3174, 0.292, 0.4834] |
| changed | `rq5_perf.legs.us-east-1.live.fluentbit.end_to_end_s_p100` | [5.8031, 5.4194, 5.9492] | [5.5344, 5.2422, 5.826] |
| changed | `rq5_perf.legs.us-east-1.live.fluentbit.end_to_end_s_p50` | [3.154, 3.054, 3.2984] | [3.1782, 2.8923, 3.1871] |
| changed | `rq5_perf.legs.us-east-1.live.fluentbit.end_to_end_s_p90` | [4.8263, 4.655, 4.9919] | [4.7878, 4.4926, 4.8681] |
| changed | `rq5_perf.legs.us-east-1.live.fluentbit.end_to_end_s_p99` | [5.405, 5.2713, 5.5511] | [5.4008, 5.106, 5.4268] |
| changed | `rq5_perf.legs.us-east-1.live.fluentbit.object_visible_s_p50` | [3.154, 3.054, 3.2984] | [3.1782, 2.8923, 3.1871] |
| changed | `rq5_perf.legs.us-east-1.live.vector.commit_s_p50` | [5.1525, 5.1505, 5.1676] | [5.1553, 5.1374, 5.1939] |
| changed | `rq5_perf.legs.us-east-1.live.vector.end_to_end_s_p100` | [9.1604, 9.158, 9.1764] | [9.1729, 9.152, 9.2036] |
| changed | `rq5_perf.legs.us-east-1.live.vector.end_to_end_s_p50` | [5.1525, 5.1505, 5.1676] | [5.1553, 5.1374, 5.1939] |
| changed | `rq5_perf.legs.us-east-1.live.vector.end_to_end_s_p90` | [7.6552, 7.6528, 7.6704] | [7.6611, 7.6403, 7.6973] |
| changed | `rq5_perf.legs.us-east-1.live.vector.end_to_end_s_p99` | [8.9559, 8.9541, 8.9706] | [8.9623, 8.9428, 8.9979] |
| changed | `rq5_perf.legs.us-east-1.live.vector.object_visible_s_p50` | [2.666, 2.662, 2.6789] | [2.6658, 2.6466, 2.6824] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-blake3-readback.commit_s_p50` | [2.7169, 2.6602, 2.7306] | [2.7526, 2.7369, 2.7981] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-blake3-readback.end_to_end_s_p100` | [4.8893, 4.8742, 4.9047] | [4.9741, 4.913, 5.0117] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-blake3-readback.end_to_end_s_p50` | [2.7605, 2.6928, 2.7713] | [2.8127, 2.7617, 2.8996] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-blake3-readback.end_to_end_s_p90` | [4.2861, 4.2516, 4.2978] | [4.3341, 4.3002, 4.4521] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-blake3-readback.end_to_end_s_p99` | [4.6841, 4.6683, 4.7197] | [4.7861, 4.7308, 4.8551] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-blake3-readback.object_visible_s_p50` | [2.6856, 2.6285, 2.6943] | [2.7483, 2.6384, 2.8143] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-sha256-store.commit_s_p50` | [2.6502, 2.6415, 2.6637] | [2.5925, 2.5807, 2.6807] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-sha256-store.end_to_end_s_p100` | [4.8769, 4.8064, 4.8999] | [4.8901, 4.881, 4.9722] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-sha256-store.end_to_end_s_p50` | [2.7515, 2.7205, 2.7662] | [2.698, 2.6957, 2.8058] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-sha256-store.end_to_end_s_p90` | [4.2818, 4.2306, 4.2876] | [4.2317, 4.2278, 4.3258] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-sha256-store.end_to_end_s_p99` | [4.702, 4.6731, 4.7022] | [4.6835, 4.6516, 4.7815] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-sha256-store.object_visible_s_p50` | [2.7084, 2.6568, 2.7226] | [2.675, 2.6523, 2.7632] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-size-only.commit_s_p50` | [2.6442, 2.5673, 2.6941] | [2.6465, 2.6345, 2.6612] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-size-only.end_to_end_s_p100` | [4.7735, 4.7483, 4.8748] | [4.8951, 4.768, 4.9267] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-size-only.end_to_end_s_p50` | [2.6975, 2.6443, 2.7711] | [2.7326, 2.7289, 2.7718] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-size-only.end_to_end_s_p90` | [4.2288, 4.1767, 4.3103] | [4.2444, 4.2288, 4.2621] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-size-only.end_to_end_s_p99` | [4.6398, 4.6142, 4.7306] | [4.6853, 4.6454, 4.6983] |
| changed | `rq5_perf.legs.us-east-1.live.vtop-size-only.object_visible_s_p50` | [2.6114, 2.6006, 2.706] | [2.7071, 2.6675, 2.7192] |
| added | `rq5_perf.legs.us-east-1.rejudged.[0].file` | "\u2014" | "rejudge-106-tput-vector-r4.json" |
| added | `rq5_perf.legs.us-east-1.rejudged.[0].pass` | "\u2014" | true |
| added | `rq5_perf.legs.us-east-1.rejudged.[0].run` | "\u2014" | "106-tput-vector-r4" |
| changed | `rq5_perf.legs.us-east-1.rtt_sink_ms_median.end` | 0.75 | 0.63 |
| changed | `rq5_perf.legs.us-east-1.rtt_sink_ms_median.start` | 0.63 | 0.45 |
| changed | `rq5_perf.legs.us-east-1.session` | "20261121T144252Z-use1" | "20261122T041404Z-use1" |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.archived_mib_s` | [37.733, 35.434, 40.334] | [37.723, 36.534, 39.022] |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.committed_mib_s` | [37.706, 34.173, 39.874] | [37.593, 36.601, 39.088] |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.cpu_cores_median` | 1.977 | 1.96 |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.cpu_per_gib_vs_ref_pct` | [146.2699, 122.7514, 167.1195] | [121.6143, 110.3411, 131.1978] |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.cpu_s_per_gib` | [54.204, 49.952, 58.793] | [53.101, 51.929, 54.992] |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.done_mib_s` | [37.484, 34.173, 40.531] | [37.818, 36.326, 38.774] |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.done_vs_ref_pct` | [91.1961, 74.3076, 106.7381] | [63.7923, 51.0786, 79.2237] |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.ena_allowance_exceeded_sink` | 172 | 994 |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.peak_anon_mib_median` | 1205.0156 | 1012.7266 |
| changed | `rq5_perf.legs.us-east-1.tput.confluent.sink_ec2_usd_per_gib` | 0.00051 | 0.000505 |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.archived_mib_s` | [4.834, 4.834, 4.834] | [4.834, 4.834, 4.854] |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.committed_mib_s` | [4.837, 4.836, 4.837] | [4.836, 4.832, 4.836] |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.compression_ratio_median` | 8.5488 | 8.5515 |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.cpu_cores_median` | 0.125 | 0.124 |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.cpu_per_gib_vs_ref_pct` | [20.3771, 13.6344, 21.4978] | [9.7408, 5.4385, 14.3692] |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.cpu_s_per_gib` | [26.495, 25.864, 26.687] | [26.295, 25.971, 26.425] |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.done_mib_s` | [4.834, 4.834, 4.834] | [4.834, 4.834, 4.854] |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.done_vs_ref_pct` | [-75.343, -76.4356, -74.956] | [-79.0636, -80.6887, -77.0911] |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.ena_allowance_exceeded_sink` | 4506 | 2863 |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.peak_anon_mib_median` | 123.6719 | 131.9609 |
| changed | `rq5_perf.legs.us-east-1.tput.fluentbit.raw_mib_per_object_median` | 17.094 | 17.0932 |
| changed | `rq5_perf.legs.us-east-1.tput.vector.archived_mib_s` | [9.667, 9.511, 9.831] | [9.435, 9.36, 10.085] |
| changed | `rq5_perf.legs.us-east-1.tput.vector.committed_mib_s` | [9.611, 9.485, 9.888] | [9.615, 9.238, 10.029] |
| changed | `rq5_perf.legs.us-east-1.tput.vector.compression_ratio_median` | 7.6392 | 7.6873 |
| changed | `rq5_perf.legs.us-east-1.tput.vector.cpu_cores_median` | 1.894 | 1.889 |
| changed | `rq5_perf.legs.us-east-1.tput.vector.cpu_per_gib_vs_ref_pct` | [818.2008, 766.7696, 829.2192] | [750.5154, 704.9956, 787.4652] |
| changed | `rq5_perf.legs.us-east-1.tput.vector.cpu_s_per_gib` | [202.096, 196.899, 204.103] | [203.792, 192.885, 205.748] |
| changed | `rq5_perf.legs.us-east-1.tput.vector.done_mib_s` | [9.611, 9.485, 9.848] | [9.485, 9.355, 10.029] |
| changed | `rq5_perf.legs.us-east-1.tput.vector.done_vs_ref_pct` | [-50.9768, -53.1491, -49.7166] | [-58.9198, -62.1085, -54.706] |
| changed | `rq5_perf.legs.us-east-1.tput.vector.ena_allowance_exceeded_sink` | 7248 | 5103 |
| changed | `rq5_perf.legs.us-east-1.tput.vector.peak_anon_mib_median` | 562.0508 | 559.7969 |
| changed | `rq5_perf.legs.us-east-1.tput.vector.raw_mib_per_object_median` | 16.2578 | 16.256 |
| changed | `rq5_perf.legs.us-east-1.tput.vector.sink_ec2_usd_per_gib` | 0.001989 | 0.002015 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.archived_mib_s` | [19.363, 18.115, 20.334] | [22.747, 19.9, 26.248] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.committed_mib_s` | [18.667, 18.177, 20.506] | [22.955, 20.098, 26.579] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.compression_ratio_median` | 7.9293 | 7.93 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.cpu_cores_median` | 0.425 | 0.562 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.cpu_per_gib_vs_ref_pct` | [5.3021, -0.5962, 6.5925] | [5.5006, -1.8697, 10.8614] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.cpu_s_per_gib` | [23.177, 22.807, 23.461] | [25.279, 23.513, 25.667] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.data_objects_per_gib` | 78.4025 | 77.0148 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.done_mib_s` | [18.895, 18.154, 20.334] | [22.747, 19.933, 26.288] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.done_vs_ref_pct` | [-3.6215, -8.9112, 3.7184] | [-1.4812, -15.7079, 13.8551] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.done_vs_size_only_pct` | [-2.5026, -12.3852, 5.0418] | [-7.3668, -22.9927, 7.0533] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.ena_allowance_exceeded_sink` | 3672 | 5031 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.peak_anon_mib_median` | 218.2461 | 224.3555 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.raw_mib_per_object_median` | 13.5679 | 13.7482 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.request_usd_per_gib_est` | 0.000941 | 0.000924 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-blake3-readback.sink_ec2_usd_per_gib` | 0.001012 | 0.00084 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.archived_mib_s` | [14.82, 12.289, 15.682] | [17.002, 15.912, 17.888] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.committed_mib_s` | [14.904, 12.527, 15.759] | [16.817, 15.85, 18.016] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.cpu_cores_median` | 0.694 | 0.812 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.cpu_per_gib_vs_ref_pct` | [116.2744, 104.1602, 121.3721] | [107.892, 98.8049, 118.2039] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.cpu_s_per_gib` | [47.602, 46.508, 48.724] | [49.813, 48.885, 50.416] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.data_objects_per_gib` | 78.4025 | 76.321 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.done_mib_s` | [14.864, 12.435, 15.87] | [16.941, 15.912, 17.888] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.done_vs_ref_pct` | [-24.1826, -36.5723, -19.0513] | [-26.6274, -33.2854, -19.2124] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.done_vs_size_only_pct` | [-23.3024, -35.8359, -17.96] | [-31.0108, -38.767, -26.9042] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.ena_allowance_exceeded_sink` | 3760 | 2813 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.peak_anon_mib_median` | 215.2578 | 206.6719 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.raw_mib_per_object_median` | 13.5241 | 13.8028 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.request_usd_per_gib_est` | 0.000878 | 0.000855 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-gzip6.sink_ec2_usd_per_gib` | 0.001286 | 0.001128 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.archived_mib_s` | [19.605, 19.567, 20.7] | [23.613, 21.101, 25.032] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.committed_mib_s` | [19.583, 19.301, 20.415] | [23.01, 20.886, 25.144] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.compression_ratio_median` | 7.9284 | 7.9299 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.cpu_cores_median` | 0.427 | 0.54 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.cpu_s_per_gib` | [22.01, 21.838, 23.316] | [23.961, 23.008, 24.768] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.data_objects_per_gib` | 78.4025 | 77.0148 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.done_mib_s` | [19.605, 19.302, 20.514] | [23.089, 21.101, 25.032] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.done_vs_size_only_pct` | [1.161, -9.093, 8.2073] | [-5.9741, -18.7986, 2.2883] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.ena_allowance_exceeded_sink` | 2844 | 5022 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.peak_anon_mib_median` | 233.8867 | 220.2734 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.raw_mib_per_object_median` | 13.6108 | 13.6415 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.request_usd_per_gib_est` | 0.000878 | 0.000863 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-sha256-store.sink_ec2_usd_per_gib` | 0.000975 | 0.000828 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.archived_mib_s` | [19.138, 18.12, 21.85] | [24.556, 23.643, 26.776] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.committed_mib_s` | [19.399, 18.113, 21.734] | [24.848, 23.077, 27.549] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.compression_ratio_median` | 7.9296 | 7.9298 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.cpu_cores_median` | 0.417 | 0.598 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.cpu_per_gib_vs_ref_pct` | [0.5861, -5.048, 3.5575] | [2.4832, -2.6001, 8.3099] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.cpu_s_per_gib` | [22.139, 21.415, 22.793] | [24.556, 24.008, 25.025] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.data_objects_per_gib` | 78.4025 | 76.321 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.done_mib_s` | [19.38, 18.118, 21.566] | [24.556, 23.324, 27.4] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.done_vs_ref_pct` | [-1.1477, -7.5848, 10.0026] | [6.3537, -2.2371, 23.1506] |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.ena_allowance_exceeded_sink` | 4222 | 3652 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.peak_anon_mib_median` | 229.1055 | 227.7422 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.raw_mib_per_object_median` | 13.6176 | 13.6517 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.request_usd_per_gib_est` | 0.000847 | 0.000824 |
| changed | `rq5_perf.legs.us-east-1.tput.vtop-size-only.sink_ec2_usd_per_gib` | 0.000986 | 0.000778 |
| changed | `rq5_perf.legs.us-east-1.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_scale.claims.aggregate_n8_mib_s` | [373.0875, 368.909, 377.6755] | [369.285, 366.37, 372.33] |
| changed | `rq5_scale.claims.efficiency_ci_high_max` | 1.0292 | 1.0491 |
| changed | `rq5_scale.claims.efficiency_ci_low_min` | 0.9797 | 0.9821 |
| changed | `rq5_scale.claims.efficiency_n8` | [1.0006, 0.9851, 1.0214] | [1.0194, 0.9875, 1.0449] |
| changed | `rq5_scale.claims.efficiency_point_range` | [0.9973, 1.0092] | [1.015, 1.0242] |
| changed | `rq5_scale.claims.peak_s3_attempts_per_s_n8` | 157 | 154 |
| changed | `rq5_scale.claims.per_engine_change_point_range` | [-0.6319, 1.2187] | [1.441, 2.3708] |
| changed | `rq5_scale.control_size_only.per_engine_mib_s` | [49.437, 48.9575, 49.8525] | [48.5075, 48.1845, 48.74] |
| changed | `rq5_scale.control_size_only.sha256_vs_size_only_aggregate_pct` | [-5.3755, -6.6931, -4.1013] | [-4.7065, -5.7972, -3.8132] |
| changed | `rq5_scale.control_size_only.sha256_vs_size_only_per_engine_pct` | [-5.5839, -6.6185, -4.5172] | [-4.5323, -5.694, -3.7325] |
| changed | `rq5_scale.per_n.1.aggregate_mib_s` | [46.6075, 45.9455, 47.017] | [45.28, 44.27, 46.669] |
| changed | `rq5_scale.per_n.1.ena_allowance_exceeded_sum` | 407 | 609 |
| changed | `rq5_scale.per_n.1.head_latency_ms_p50_median` | 13.02 | 13.364 |
| changed | `rq5_scale.per_n.1.host_cpu_busy_pct_median` | 25.3315 | 25.188 |
| changed | `rq5_scale.per_n.1.per_engine_cores_median` | 0.4504 | 0.4465 |
| changed | `rq5_scale.per_n.1.per_engine_mib_s` | [46.6075, 45.9455, 47.017] | [45.28, 44.27, 46.669] |
| changed | `rq5_scale.per_n.1.put_latency_ms_p50_median` | 30.4105 | 30.546 |
| changed | `rq5_scale.per_n.1.s3_attempts_per_s_median` | 16.735 | 16.275 |
| changed | `rq5_scale.per_n.1.window_mib_s_median` | 46.359 | 45.28 |
| changed | `rq5_scale.per_n.2.aggregate_mib_s` | [92.9595, 91.5925, 93.764] | [91.917, 90.5775, 93.363] |
| changed | `rq5_scale.per_n.2.efficiency` | [0.9973, 0.9797, 1.0131] | [1.015, 0.9821, 1.0447] |
| changed | `rq5_scale.per_n.2.ena_allowance_exceeded_sum` | 916 | 1222 |
| changed | `rq5_scale.per_n.2.head_latency_ms_p50_median` | 13.0075 | 13.1007 |
| changed | `rq5_scale.per_n.2.host_cpu_busy_pct_median` | 25.308 | 25.186 |
| changed | `rq5_scale.per_n.2.per_engine_change_vs_n1_pct` | [-0.6319, -1.8451, 0.9867] | [1.441, -1.7299, 4.1428] |
| changed | `rq5_scale.per_n.2.per_engine_cores_median` | 0.4507 | 0.4501 |
| changed | `rq5_scale.per_n.2.per_engine_mib_s` | [46.313, 45.882, 46.644] | [45.9325, 45.6075, 46.3435] |
| changed | `rq5_scale.per_n.2.put_latency_ms_p50_median` | 30.374 | 30.6863 |
| changed | `rq5_scale.per_n.2.s3_attempts_per_s_median` | 33.15 | 32.85 |
| changed | `rq5_scale.per_n.2.s3_peak_attempts_per_s_max` | 42 | 40 |
| changed | `rq5_scale.per_n.2.start_skew_s_max` | 0.0113 | 0.0126 |
| changed | `rq5_scale.per_n.2.window_mib_s_median` | 92.497 | 91.4785 |
| changed | `rq5_scale.per_n.2.window_overlap_fraction_min` | 0.9793 | 0.979 |
| changed | `rq5_scale.per_n.4.aggregate_mib_s` | [188.138, 185.461, 190.121] | [185.5, 182.88, 186.702] |
| changed | `rq5_scale.per_n.4.efficiency` | [1.0092, 0.9902, 1.0292] | [1.0242, 0.989, 1.0491] |
| changed | `rq5_scale.per_n.4.ena_allowance_exceeded_sum` | 2805 | 3124 |
| changed | `rq5_scale.per_n.4.head_latency_ms_p50_median` | 12.9455 | 12.9445 |
| changed | `rq5_scale.per_n.4.host_cpu_busy_pct_median` | 25.1178 | 25.0832 |
| changed | `rq5_scale.per_n.4.per_engine_change_vs_n1_pct` | [1.2187, 0.0223, 2.9026] | [2.3708, -0.9518, 4.8062] |
| changed | `rq5_scale.per_n.4.per_engine_cores_median` | 0.453 | 0.4501 |
| changed | `rq5_scale.per_n.4.per_engine_mib_s` | [47.1755, 46.768, 47.474] | [46.3535, 45.8205, 46.6105] |
| changed | `rq5_scale.per_n.4.put_latency_ms_p50_median` | 30.1513 | 30.4975 |
| changed | `rq5_scale.per_n.4.s3_attempts_per_s_median` | 66.005 | 65.36 |
| changed | `rq5_scale.per_n.4.s3_peak_attempts_per_s_max` | 79 | 80 |
| changed | `rq5_scale.per_n.4.start_skew_s_max` | 0.0102 | 0.0314 |
| changed | `rq5_scale.per_n.4.window_mib_s_median` | 186.7755 | 184.1845 |
| changed | `rq5_scale.per_n.4.window_overlap_fraction_min` | 0.9586 | 0.9508 |
| changed | `rq5_scale.per_n.8.aggregate_mib_s` | [373.0875, 368.909, 377.6755] | [369.285, 366.37, 372.33] |
| changed | `rq5_scale.per_n.8.efficiency` | [1.0006, 0.9851, 1.0214] | [1.0194, 0.9875, 1.0449] |
| changed | `rq5_scale.per_n.8.ena_allowance_exceeded_sum` | 4557 | 4142 |
| changed | `rq5_scale.per_n.8.head_latency_ms_p50_median` | 12.9642 | 13.0562 |
| changed | `rq5_scale.per_n.8.host_cpu_busy_pct_median` | 25.3178 | 25.1245 |
| changed | `rq5_scale.per_n.8.per_engine_change_vs_n1_pct` | [0.148, -0.9983, 1.7187] | [2.2725, -1.1919, 4.6894] |
| changed | `rq5_scale.per_n.8.per_engine_cores_median` | 0.4535 | 0.4494 |
| changed | `rq5_scale.per_n.8.per_engine_mib_s` | [46.6765, 46.402, 46.983] | [46.309, 45.836, 46.478] |
| changed | `rq5_scale.per_n.8.put_latency_ms_p50_median` | 30.194 | 30.6668 |
| changed | `rq5_scale.per_n.8.s3_attempts_per_s_median` | 130.41 | 128.82 |
| changed | `rq5_scale.per_n.8.s3_peak_attempts_per_s_max` | 157 | 154 |
| changed | `rq5_scale.per_n.8.start_skew_s_max` | 0.0203 | 0.0121 |
| changed | `rq5_scale.per_n.8.window_mib_s_median` | 369.8415 | 366.3425 |
| changed | `rq5_scale.per_n.8.window_overlap_fraction_min` | 0.9478 | 0.9657 |
| changed | `rq5_scale.session` | "20261121T153823Z-scale" | "20261122T032729Z-scale" |
| changed | `rq5_scale.vtop` | "0.10.2" | "0.10.4" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-al2023-linux-v0102.agree` | 53 | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-al2023-linux-v0102.comparable` | 53 | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-al2023-linux-v0102.differ` | [] | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-minio-v0102.agree` | 53 | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-minio-v0102.comparable` | 53 | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-minio-v0102.differ` | [] | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-minio-v0102-rep2.agree` | 53 | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-minio-v0102-rep2.comparable` | 53 | "\u2014" |
| removed | `rq5_tamper.agreement_with_local.20261121-local-minio-v0102-rep2.differ` | [] | "\u2014" |
| added | `rq5_tamper.agreement_with_local.20261122-local-al2023-linux-v0104.agree` | "\u2014" | 53 |
| added | `rq5_tamper.agreement_with_local.20261122-local-al2023-linux-v0104.comparable` | "\u2014" | 53 |
| added | `rq5_tamper.agreement_with_local.20261122-local-al2023-linux-v0104.differ` | "\u2014" | [] |
| added | `rq5_tamper.agreement_with_local.20261122-local-minio-v0104.agree` | "\u2014" | 53 |
| added | `rq5_tamper.agreement_with_local.20261122-local-minio-v0104.comparable` | "\u2014" | 53 |
| added | `rq5_tamper.agreement_with_local.20261122-local-minio-v0104.differ` | "\u2014" | [] |
| added | `rq5_tamper.agreement_with_local.20261122-local-minio-v0104-rep2.agree` | "\u2014" | 53 |
| added | `rq5_tamper.agreement_with_local.20261122-local-minio-v0104-rep2.comparable` | "\u2014" | 53 |
| added | `rq5_tamper.agreement_with_local.20261122-local-minio-v0104-rep2.differ` | "\u2014" | [] |
| changed | `rq5_tamper.cases_by_hypothesis.H2.5.caught` | 11 | 13 |
| changed | `rq5_tamper.cases_by_hypothesis.H2.5.caught + NOT CAUGHT (limit)` | 6 | 4 |
| changed | `rq5_tamper.checks.NOT CAUGHT` | 30 | 26 |
| changed | `rq5_tamper.checks.caught` | 70 | 74 |
| added | `rq5_tamper.engine_changes.[0].case` | "\u2014" | "MAC-SWAP-OFF" |
| added | `rq5_tamper.engine_changes.[0].observed` | "\u2014" | "failed" |
| added | `rq5_tamper.engine_changes.[0].verifier` | "\u2014" | "vm_noledger" |
| added | `rq5_tamper.engine_changes.[0].why` | "\u2014" | "#882 (v0.10.3): verify-manifest binds a manifest to the key it names" |
| added | `rq5_tamper.engine_changes.[1].case` | "\u2014" | "MAC-SWAP-OFF" |
| added | `rq5_tamper.engine_changes.[1].observed` | "\u2014" | "failed" |
| added | `rq5_tamper.engine_changes.[1].verifier` | "\u2014" | "vm_ledger" |
| added | `rq5_tamper.engine_changes.[1].why` | "\u2014" | "#882 (v0.10.3): verify-manifest binds a manifest to the key it names" |
| added | `rq5_tamper.engine_changes.[2].case` | "\u2014" | "MAC-SWAP-ON" |
| added | `rq5_tamper.engine_changes.[2].observed` | "\u2014" | "failed" |
| added | `rq5_tamper.engine_changes.[2].verifier` | "\u2014" | "vm_noledger" |
| added | `rq5_tamper.engine_changes.[2].why` | "\u2014" | "#882 (v0.10.3): verify-manifest binds a manifest to the key it names" |
| added | `rq5_tamper.engine_changes.[3].case` | "\u2014" | "MAC-SWAP-ON" |
| added | `rq5_tamper.engine_changes.[3].observed` | "\u2014" | "failed" |
| added | `rq5_tamper.engine_changes.[3].verifier` | "\u2014" | "vm_ledger" |
| added | `rq5_tamper.engine_changes.[3].why` | "\u2014" | "#882 (v0.10.3): verify-manifest binds a manifest to the key it names" |
| changed | `rq5_tamper.limit_cases` | ["AUD-RECOMPUTE-LEAKED", "AUD-RECOMPUTE-UNKEYED", "AUD-TRUNCATE", "COV-DELETE-END", "MAC-FORGE-OFF-B", "MAC-FORGE-OFF-BL | ["AUD-RECOMPUTE-LEAKED", "AUD-RECOMPUTE-UNKEYED", "AUD-TRUNCATE", "COV-DELETE-END", "MAC-FORGE-OFF-B", "MAC-FORGE-OFF-BL |
| removed | `rq5_tamper.local_differs.20261121-local-al2023-linux-v0102` | ["VER-SUSPEND-RUNNING"] | "\u2014" |
| removed | `rq5_tamper.local_differs.20261121-local-minio-v0102` | ["VER-SUSPEND-RUNNING"] | "\u2014" |
| removed | `rq5_tamper.local_differs.20261121-local-minio-v0102-rep2` | ["VER-SUSPEND-RUNNING"] | "\u2014" |
| added | `rq5_tamper.local_differs.20261122-local-al2023-linux-v0104` | "\u2014" | ["VER-SUSPEND-RUNNING"] |
| added | `rq5_tamper.local_differs.20261122-local-minio-v0104` | "\u2014" | ["MAC-SWAP-OFF", "MAC-SWAP-ON", "VER-SUSPEND-RUNNING"] |
| added | `rq5_tamper.local_differs.20261122-local-minio-v0104-rep2` | "\u2014" | ["MAC-SWAP-OFF", "MAC-SWAP-ON", "VER-SUSPEND-RUNNING"] |
| removed | `rq5_tamper.local_runs.20261121-local-al2023-linux-v0102.outcomes.DIFFERS` | 1 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-al2023-linux-v0102.outcomes.NOT CAUGHT (limit)` | 7 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-al2023-linux-v0102.outcomes.as expected` | 3 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-al2023-linux-v0102.outcomes.caught` | 28 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-al2023-linux-v0102.outcomes.caught + NOT CAUGHT (limit)` | 10 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-al2023-linux-v0102.outcomes.prevented` | 6 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-al2023-linux-v0102.where` | "local MinIO" | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102.outcomes.DIFFERS` | 1 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102.outcomes.NOT CAUGHT (limit)` | 7 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102.outcomes.as expected` | 3 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102.outcomes.caught` | 28 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102.outcomes.caught + NOT CAUGHT (limit)` | 10 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102.outcomes.prevented` | 6 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102.where` | "local MinIO" | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102-rep2.outcomes.DIFFERS` | 1 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102-rep2.outcomes.NOT CAUGHT (limit)` | 7 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102-rep2.outcomes.as expected` | 3 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102-rep2.outcomes.caught` | 28 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102-rep2.outcomes.caught + NOT CAUGHT (limit)` | 10 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102-rep2.outcomes.prevented` | 6 | "\u2014" |
| removed | `rq5_tamper.local_runs.20261121-local-minio-v0102-rep2.where` | "local MinIO" | "\u2014" |
| added | `rq5_tamper.local_runs.20261122-local-al2023-linux-v0104.outcomes.DIFFERS` | "\u2014" | 1 |
| added | `rq5_tamper.local_runs.20261122-local-al2023-linux-v0104.outcomes.NOT CAUGHT (limit)` | "\u2014" | 7 |
| added | `rq5_tamper.local_runs.20261122-local-al2023-linux-v0104.outcomes.as expected` | "\u2014" | 3 |
| added | `rq5_tamper.local_runs.20261122-local-al2023-linux-v0104.outcomes.caught` | "\u2014" | 30 |
| added | `rq5_tamper.local_runs.20261122-local-al2023-linux-v0104.outcomes.caught + NOT CAUGHT (limit)` | "\u2014" | 8 |
| added | `rq5_tamper.local_runs.20261122-local-al2023-linux-v0104.outcomes.prevented` | "\u2014" | 6 |
| added | `rq5_tamper.local_runs.20261122-local-al2023-linux-v0104.where` | "\u2014" | "local MinIO" |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104.outcomes.DIFFERS` | "\u2014" | 3 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104.outcomes.NOT CAUGHT (limit)` | "\u2014" | 7 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104.outcomes.as expected` | "\u2014" | 3 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104.outcomes.caught` | "\u2014" | 28 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104.outcomes.caught + NOT CAUGHT (limit)` | "\u2014" | 8 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104.outcomes.prevented` | "\u2014" | 6 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104.where` | "\u2014" | "local MinIO" |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104-rep2.outcomes.DIFFERS` | "\u2014" | 3 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104-rep2.outcomes.NOT CAUGHT (limit)` | "\u2014" | 7 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104-rep2.outcomes.as expected` | "\u2014" | 3 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104-rep2.outcomes.caught` | "\u2014" | 28 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104-rep2.outcomes.caught + NOT CAUGHT (limit)` | "\u2014" | 8 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104-rep2.outcomes.prevented` | "\u2014" | 6 |
| added | `rq5_tamper.local_runs.20261122-local-minio-v0104-rep2.where` | "\u2014" | "local MinIO" |
| removed | `rq5_tamper.local_suspend_running_writes.20261121-local-al2023-linux-v0102` | 10 | "\u2014" |
| removed | `rq5_tamper.local_suspend_running_writes.20261121-local-minio-v0102` | 10 | "\u2014" |
| removed | `rq5_tamper.local_suspend_running_writes.20261121-local-minio-v0102-rep2` | 10 | "\u2014" |
| added | `rq5_tamper.local_suspend_running_writes.20261122-local-al2023-linux-v0104` | "\u2014" | 1 |
| added | `rq5_tamper.local_suspend_running_writes.20261122-local-minio-v0104` | "\u2014" | 1 |
| added | `rq5_tamper.local_suspend_running_writes.20261122-local-minio-v0104-rep2` | "\u2014" | 1 |
| changed | `rq5_tamper.outcomes.caught` | 27 | 29 |
| changed | `rq5_tamper.outcomes.caught + NOT CAUGHT (limit)` | 10 | 8 |
| added | `rq5_tamper.outcomes_raw.DIFFERS` | "\u2014" | 2 |
| added | `rq5_tamper.outcomes_raw.NOT CAUGHT (limit)` | "\u2014" | 7 |
| added | `rq5_tamper.outcomes_raw.as expected` | "\u2014" | 3 |
| added | `rq5_tamper.outcomes_raw.caught` | "\u2014" | 27 |
| added | `rq5_tamper.outcomes_raw.caught + NOT CAUGHT (limit)` | "\u2014" | 8 |
| added | `rq5_tamper.outcomes_raw.prevented` | "\u2014" | 6 |
| added | `rq5_tamper.outcomes_raw.skipped` | "\u2014" | 2 |
| changed | `rq5_tamper.rows.[2].caught_by` | ["S", "R"] | ["S", "V", "V$_\\ell$", "R"] |
| changed | `rq5_tamper.rows.[2].not_caught_by` | ["V", "V$_\\ell$"] | [] |
| changed | `rq5_tamper.rows.[2].outcomes` | ["caught + NOT CAUGHT (limit)", "caught + NOT CAUGHT (limit)"] | ["caught", "caught"] |
| changed | `rq5_tamper.run_id` | "20261121T143301Z-i-0938589e4d715ff8d" | "20261122T021852Z-i-0e178e36bc660a51f" |
| changed | `rq5_tamper.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_tamper.vtop_commit` | "0abf11696e47f4db871bd4c2cda882756c114390" | "be5b419ed0658e217284e8324caaff31f39622c5" |
| changed | `rq5_wan.baseline_runs` | "research/experiments/rq5-cloud/cost/results/20261121T002644Z-i-07a97d2f7a612888e/runs.jsonl" | "research/experiments/rq5-cloud/cost/results/20261122T021852Z-i-0d197e09de6af68de/runs.jsonl" |
| changed | `rq5_wan.claims.cap2_store_of_cap_range` | [0.697, 0.822] | [0.699, 0.821] |
| changed | `rq5_wan.claims.loss5_x_native_range` | [9.26, 12.25] | [7.95, 12.57] |
| changed | `rq5_wan.claims.loss_failed_batches` | 0 | 1 |
| changed | `rq5_wan.claims.partition_first_commit_after_restore_s_range` | [2.013, 3.53] | [2.031, 4.696] |
| changed | `rq5_wan.claims.readback_change_pct_range` | [-35.2791, -23.8361] | [-36.4047, -22.9963] |
| changed | `rq5_wan.claims.readback_drain_tc2_over_cap2_range` | [1.68, 1.79] | [1.69, 1.79] |
| changed | `rq5_wan.claims.readback_slowdown_increases_with_rtt` | true | false |
| changed | `rq5_wan.claims.rtt_ms_range` | [66.435, 196.06] | [66.325, 196.05] |
| changed | `rq5_wan.claims.sigkill_restart_drain_s_range` | [6.34, 15.509] | [6.44, 16.239] |
| changed | `rq5_wan.claims.size_only_mib_s_range` | [6.664, 11.5665] | [6.756, 11.7845] |
| changed | `rq5_wan.claims.size_only_vs_us_east_1_pct_range` | [-66.3833, -41.6526] | [-67.488, -43.2892] |
| changed | `rq5_wan.claims.store_change_pct_range` | [-13.9387, -1.0332] | [-9.9652, -2.8851] |
| changed | `rq5_wan.claims.tc10_store_of_cap_range` | [0.299, 0.474] | [0.307, 0.466] |
| changed | `rq5_wan.claims.tc2_store_of_cap_range` | [0.667, 0.818] | [0.658, 0.82] |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/blake3-readback.drain_s` | 13.76 | 13.666 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/blake3-readback.mib_s` | 5.088 | 5.123 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/blake3-readback.of_cap` | 0.299 | 0.301 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/blake3-readback.rx_mbit_s` | 3.218 | 3.221 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/blake3-readback.tx_mbit_s` | 3.203 | 3.248 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/blake3-readback.upload_body_mbit_s` | 2.99 | 3.01 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/sha256-store.drain_s` | 11.008 | 10.846 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/sha256-store.mib_s` | 6.36 | 6.455 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/sha256-store.of_cap` | 0.374 | 0.379 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/sha256-store.rx_mbit_s` | 0.087 | 0.093 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/sha256-store.tx_mbit_s` | 3.997 | 4.023 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap10/sha256-store.upload_body_mbit_s` | 3.737 | 3.793 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/blake3-readback.drain_s` | 30.73 | 30.474 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/blake3-readback.mib_s` | 2.278 | 2.298 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/blake3-readback.of_cap` | 0.669 | 0.675 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/blake3-readback.rx_mbit_s` | 1.453 | 1.45 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/blake3-readback.tx_mbit_s` | 1.451 | 1.466 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/blake3-readback.upload_body_mbit_s` | 1.339 | 1.35 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/sha256-store.drain_s` | 27.912 | 27.828 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/sha256-store.mib_s` | 2.508 | 2.516 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/sha256-store.of_cap` | 0.737 | 0.739 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/sha256-store.rx_mbit_s` | 0.047 | 0.045 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/sha256-store.tx_mbit_s` | 1.589 | 1.594 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.cap2/sha256-store.upload_body_mbit_s` | 1.474 | 1.478 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/blake3-readback.calibration` | [6.487, 6.639] | [6.114, 6.483] |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/blake3-readback.drain_s` | 19.864 | 20.355 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/blake3-readback.mib_s` | 3.525 | 3.44 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/blake3-readback.of_cap` | 0.207 | 0.202 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/blake3-readback.rx_mbit_s` | 2.42 | 2.353 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/blake3-readback.tx_mbit_s` | 2.28 | 2.232 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/blake3-readback.upload_body_mbit_s` | 2.071 | 2.021 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/sha256-store.calibration` | [6.487, 6.639] | [6.114, 6.483] |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/sha256-store.drain_s` | 11.804 | 11.538 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/sha256-store.mib_s` | 5.932 | 6.068 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/sha256-store.of_cap` | 0.348 | 0.356 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/sha256-store.rx_mbit_s` | 0.095 | 0.109 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/sha256-store.tx_mbit_s` | 3.726 | 3.775 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc10/sha256-store.upload_body_mbit_s` | 3.485 | 3.565 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/blake3-readback.calibration` | [1.815, 1.865] | [1.827, 1.87] |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/blake3-readback.drain_s` | 51.604 | 51.475 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/blake3-readback.mib_s` | 1.357 | 1.36 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/blake3-readback.of_cap` | 0.399 | 0.4 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/blake3-readback.rx_mbit_s` | 0.884 | 0.882 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/blake3-readback.tx_mbit_s` | 0.877 | 0.874 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/blake3-readback.upload_body_mbit_s` | 0.797 | 0.799 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/sha256-store.calibration` | [1.815, 1.865] | [1.827, 1.87] |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/sha256-store.drain_s` | 28.207 | 28.293 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/sha256-store.mib_s` | 2.482 | 2.475 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/sha256-store.of_cap` | 0.729 | 0.727 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/sha256-store.tx_mbit_s` | 1.552 | 1.555 |
| changed | `rq5_wan.regions.ap-northeast-1.bandwidth.tc2/sha256-store.upload_body_mbit_s` | 1.458 | 1.454 |
| changed | `rq5_wan.regions.ap-northeast-1.commit` | "0abf11696e47f4db871bd4c2cda882756c114390" | "be5b419ed0658e217284e8324caaff31f39622c5" |
| changed | `rq5_wan.regions.ap-northeast-1.head_bucket_ms` | 149.98 | 157.18 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.jitter.drain_s` | 12.862 | 12.835 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.jitter.mib_s` | 5.444 | 5.455 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.jitter.x_native` | 1.38 | 1.35 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.loss1.drain_s` | 26.96 | 32.468 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.loss1.mib_s` | 2.597 | 2.156 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.loss1.x_native` | 2.88 | 3.41 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.loss5.drain_s` | 113.046 | 119.832 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.loss5.failed_batches` | 0 | 1 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.loss5.mib_s` | 0.619 | 0.584 |
| changed | `rq5_wan.regions.ap-northeast-1.loss.loss5.x_native` | 12.09 | 12.57 |
| changed | `rq5_wan.regions.ap-northeast-1.native.blake3-readback.cpu_s` | 0.803 | 0.824 |
| changed | `rq5_wan.regions.ap-northeast-1.native.blake3-readback.drain_s` | 12.978 | 12.853 |
| changed | `rq5_wan.regions.ap-northeast-1.native.blake3-readback.mib_s` | [5.395, 5.19, 5.669] | [5.4475, 5.3215, 5.531] |
| changed | `rq5_wan.regions.ap-northeast-1.native.blake3-readback.tx_mbit_s` | 3.418 | 3.441 |
| changed | `rq5_wan.regions.ap-northeast-1.native.blake3-readback.vs_size_only_pct` | [-32.4612, -34.6997, -28.9434] | [-31.8168, -33.7568, -29.6624] |
| changed | `rq5_wan.regions.ap-northeast-1.native.blake3-readback.vs_us_east_1_pct` | [-70.3343, -71.4616, -68.8157] | [-71.0809, -71.8397, -70.5359] |
| changed | `rq5_wan.regions.ap-northeast-1.native.sha256-store.cpu_s` | 0.776 | 0.748 |
| changed | `rq5_wan.regions.ap-northeast-1.native.sha256-store.drain_s` | 9.354 | 9.535 |
| changed | `rq5_wan.regions.ap-northeast-1.native.sha256-store.mib_s` | [7.4855, 7.363, 7.5605] | [7.343, 7.1945, 7.4765] |
| changed | `rq5_wan.regions.ap-northeast-1.native.sha256-store.tx_mbit_s` | 4.699 | 4.611 |
| changed | `rq5_wan.regions.ap-northeast-1.native.sha256-store.vs_size_only_pct` | [-6.2907, -8.0823, -3.4553] | [-8.0919, -10.25, -4.8053] |
| changed | `rq5_wan.regions.ap-northeast-1.native.sha256-store.vs_us_east_1_pct` | [-62.8142, -63.5882, -60.977] | [-64.2224, -65.0001, -63.3234] |
| changed | `rq5_wan.regions.ap-northeast-1.native.size-only.cpu_s` | 0.779 | 0.763 |
| changed | `rq5_wan.regions.ap-northeast-1.native.size-only.drain_s` | 8.765 | 8.764 |
| changed | `rq5_wan.regions.ap-northeast-1.native.size-only.mib_s` | [7.988, 7.739, 8.093] | [7.9895, 7.783, 8.075] |
| changed | `rq5_wan.regions.ap-northeast-1.native.size-only.tx_mbit_s` | 4.956 | 4.963 |
| changed | `rq5_wan.regions.ap-northeast-1.native.size-only.vs_us_east_1_pct` | [-59.7044, -61.5694, -58.4281] | [-61.552, -62.6715, -60.9705] |
| changed | `rq5_wan.regions.ap-northeast-1.native_put_get_mbit_s` | [10.856, 9.174] | [12.128, 15.581] |
| changed | `rq5_wan.regions.ap-northeast-1.partition.drain_after_restore_s` | 9.657 | 9.485 |
| changed | `rq5_wan.regions.ap-northeast-1.partition.first_commit_after_restore_s` | 3.139 | 2.697 |
| changed | `rq5_wan.regions.ap-northeast-1.partition.tx_resumed_after_restore_s` | 0.946 | 1.047 |
| changed | `rq5_wan.regions.ap-northeast-1.policed.policed.drain_s` | 559.8369 | 535.8947 |
| changed | `rq5_wan.regions.ap-northeast-1.policed.policed.seal_to_commit_max_s` | 299.53 | 294.917 |
| changed | `rq5_wan.regions.ap-northeast-1.policed.policed.tx_mbit_s` | 0.501 | 0.527 |
| changed | `rq5_wan.regions.ap-northeast-1.policed.policed-fixed.tx_mbit_s` | 0.706 | 0.696 |
| changed | `rq5_wan.regions.ap-northeast-1.rtt_ms_end` | 148.72 | 147.6 |
| changed | `rq5_wan.regions.ap-northeast-1.rtt_ms_per_run_median` | 147.93 | 146.86 |
| changed | `rq5_wan.regions.ap-northeast-1.rtt_ms_start` | 147.01 | 148.14 |
| changed | `rq5_wan.regions.ap-northeast-1.sigkill.blake3-readback.restart_drain_s` | 13.2 | 12.083 |
| changed | `rq5_wan.regions.ap-northeast-1.sigkill.sha256-store.restart_drain_s` | 9.95 | 9.302 |
| changed | `rq5_wan.regions.ap-northeast-1.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/blake3-readback.drain_s` | 16.916 | 16.752 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/blake3-readback.mib_s` | 4.139 | 4.18 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/blake3-readback.of_cap` | 0.243 | 0.246 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/blake3-readback.rx_mbit_s` | 2.617 | 2.646 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/blake3-readback.tx_mbit_s` | 2.625 | 2.635 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/blake3-readback.upload_body_mbit_s` | 2.432 | 2.456 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/sha256-store.drain_s` | 12.754 | 12.8 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/sha256-store.mib_s` | 5.49 | 5.47 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/sha256-store.of_cap` | 0.323 | 0.321 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/sha256-store.rx_mbit_s` | 0.077 | 0.082 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/sha256-store.tx_mbit_s` | 3.423 | 3.417 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap10/sha256-store.upload_body_mbit_s` | 3.225 | 3.214 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/blake3-readback.drain_s` | 32.748 | 33.282 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/blake3-readback.mib_s` | 2.138 | 2.104 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/blake3-readback.of_cap` | 0.628 | 0.618 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/blake3-readback.rx_mbit_s` | 1.35 | 1.345 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/blake3-readback.tx_mbit_s` | 1.364 | 1.334 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/blake3-readback.upload_body_mbit_s` | 1.256 | 1.236 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/sha256-store.drain_s` | 29.514 | 29.445 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/sha256-store.mib_s` | 2.372 | 2.378 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/sha256-store.of_cap` | 0.697 | 0.699 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/sha256-store.rx_mbit_s` | 0.042 | 0.049 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/sha256-store.tx_mbit_s` | 1.502 | 1.504 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.cap2/sha256-store.upload_body_mbit_s` | 1.394 | 1.397 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/blake3-readback.calibration` | [5.765, 6.017] | [5.723, 5.655] |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/blake3-readback.drain_s` | 24.572 | 24.148 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/blake3-readback.mib_s` | 2.849 | 2.899 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/blake3-readback.of_cap` | 0.167 | 0.17 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/blake3-readback.rx_mbit_s` | 2.007 | 2.065 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/blake3-readback.tx_mbit_s` | 1.84 | 1.884 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/blake3-readback.upload_body_mbit_s` | 1.674 | 1.704 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/sha256-store.calibration` | [5.765, 6.017] | [5.723, 5.655] |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/sha256-store.drain_s` | 13.778 | 13.398 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/sha256-store.mib_s` | 5.082 | 5.226 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/sha256-store.of_cap` | 0.299 | 0.307 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/sha256-store.rx_mbit_s` | 0.08 | 0.091 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/sha256-store.tx_mbit_s` | 3.192 | 3.254 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc10/sha256-store.upload_body_mbit_s` | 2.986 | 3.07 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/blake3-readback.calibration` | [1.785, 1.85] | [1.787, 1.847] |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/blake3-readback.drain_s` | 56.925 | 56.372 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/blake3-readback.mib_s` | 1.23 | 1.242 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/blake3-readback.of_cap` | 0.361 | 0.365 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/blake3-readback.rx_mbit_s` | 0.804 | 0.827 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/blake3-readback.tx_mbit_s` | 0.795 | 0.8 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/blake3-readback.upload_body_mbit_s` | 0.723 | 0.73 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/sha256-store.calibration` | [1.785, 1.85] | [1.787, 1.847] |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/sha256-store.drain_s` | 30.823 | 31.249 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/sha256-store.mib_s` | 2.272 | 2.241 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/sha256-store.of_cap` | 0.667 | 0.658 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/sha256-store.tx_mbit_s` | 1.416 | 1.41 |
| changed | `rq5_wan.regions.ap-southeast-2.bandwidth.tc2/sha256-store.upload_body_mbit_s` | 1.335 | 1.316 |
| changed | `rq5_wan.regions.ap-southeast-2.commit` | "0abf11696e47f4db871bd4c2cda882756c114390" | "be5b419ed0658e217284e8324caaff31f39622c5" |
| changed | `rq5_wan.regions.ap-southeast-2.head_bucket_ms` | 203.9 | 199.99 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.jitter.drain_s` | 13.319 | 12.243 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.jitter.mib_s` | 5.257 | 5.719 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.jitter.x_native` | 1.18 | 1.08 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.loss1.drain_s` | 57.412 | 41.209 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.loss1.mib_s` | 1.22 | 1.699 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.loss1.x_native` | 5.07 | 3.65 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.loss5.drain_s` | 138.694 | 136.471 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.loss5.mib_s` | 0.505 | 0.513 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.loss5.sdk_retries` | 2 | 1 |
| changed | `rq5_wan.regions.ap-southeast-2.loss.loss5.x_native` | 12.25 | 12.08 |
| changed | `rq5_wan.regions.ap-southeast-2.native.blake3-readback.cpu_s` | 0.819 | 0.838 |
| changed | `rq5_wan.regions.ap-southeast-2.native.blake3-readback.drain_s` | 16.235 | 16.295 |
| changed | `rq5_wan.regions.ap-southeast-2.native.blake3-readback.mib_s` | [4.313, 4.227, 4.3835] | [4.2965, 4.184, 4.3665] |
| changed | `rq5_wan.regions.ap-southeast-2.native.blake3-readback.tx_mbit_s` | 2.716 | 2.719 |
| changed | `rq5_wan.regions.ap-southeast-2.native.blake3-readback.vs_size_only_pct` | [-35.2791, -38.386, -31.1635] | [-36.4047, -38.2962, -32.0724] |
| changed | `rq5_wan.regions.ap-southeast-2.native.blake3-readback.vs_us_east_1_pct` | [-76.284, -77.0614, -75.6348] | [-77.1912, -77.8505, -76.7566] |
| changed | `rq5_wan.regions.ap-southeast-2.native.sha256-store.cpu_s` | 0.767 | 0.792 |
| changed | `rq5_wan.regions.ap-southeast-2.native.sha256-store.drain_s` | 11.319 | 11.296 |
| changed | `rq5_wan.regions.ap-southeast-2.native.sha256-store.mib_s` | [6.186, 5.74, 6.293] | [6.198, 5.971, 6.249] |
| changed | `rq5_wan.regions.ap-southeast-2.native.sha256-store.tx_mbit_s` | 3.849 | 3.87 |
| changed | `rq5_wan.regions.ap-southeast-2.native.sha256-store.vs_size_only_pct` | [-7.1729, -15.4914, -1.3233] | [-8.2593, -11.7956, -2.3664] |
| changed | `rq5_wan.regions.ap-southeast-2.native.sha256-store.vs_us_east_1_pct` | [-69.2697, -71.5309, -67.8047] | [-69.8012, -70.9531, -69.3124] |
| changed | `rq5_wan.regions.ap-southeast-2.native.size-only.cpu_s` | 0.771 | 0.794 |
| changed | `rq5_wan.regions.ap-southeast-2.native.size-only.drain_s` | 10.516 | 10.364 |
| changed | `rq5_wan.regions.ap-southeast-2.native.size-only.mib_s` | [6.664, 6.272, 6.8795] | [6.756, 6.3115, 6.8544] |
| changed | `rq5_wan.regions.ap-southeast-2.native.size-only.tx_mbit_s` | 4.155 | 4.223 |
| changed | `rq5_wan.regions.ap-southeast-2.native.size-only.vs_us_east_1_pct` | [-66.3833, -68.7822, -64.5918] | [-67.488, -69.5824, -66.9306] |
| changed | `rq5_wan.regions.ap-southeast-2.native_put_get_mbit_s` | [9.277, 7.658] | [5.298, 7.712] |
| changed | `rq5_wan.regions.ap-southeast-2.partition.drain_after_restore_s` | 11.36 | 12.614 |
| changed | `rq5_wan.regions.ap-southeast-2.partition.first_commit_after_restore_s` | 3.53 | 4.696 |
| changed | `rq5_wan.regions.ap-southeast-2.partition.tx_resumed_after_restore_s` | 1.132 | 2.239 |
| changed | `rq5_wan.regions.ap-southeast-2.policed.policed.drain_s` | 608.864 | 644.8468 |
| changed | `rq5_wan.regions.ap-southeast-2.policed.policed.seal_to_commit_max_s` | 308.194 | 329.396 |
| changed | `rq5_wan.regions.ap-southeast-2.policed.policed.tx_mbit_s` | 0.465 | 0.439 |
| changed | `rq5_wan.regions.ap-southeast-2.policed.policed-fixed.tx_mbit_s` | 0.643 | 0.665 |
| changed | `rq5_wan.regions.ap-southeast-2.rtt_ms_end` | 196.07 | 196.05 |
| changed | `rq5_wan.regions.ap-southeast-2.rtt_ms_per_run_median` | 196.06 | 196.05 |
| changed | `rq5_wan.regions.ap-southeast-2.rtt_ms_start` | 196.43 | 195.67 |
| changed | `rq5_wan.regions.ap-southeast-2.sigkill.blake3-readback.restart_drain_s` | 15.509 | 16.239 |
| changed | `rq5_wan.regions.ap-southeast-2.sigkill.sha256-store.restart_drain_s` | 11.931 | 11.193 |
| changed | `rq5_wan.regions.ap-southeast-2.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/blake3-readback.drain_s` | 9.856 | 9.705 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/blake3-readback.mib_s` | 7.104 | 7.214 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/blake3-readback.of_cap` | 0.417 | 0.424 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/blake3-readback.rx_mbit_s` | 4.102 | 4.517 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/blake3-readback.tx_mbit_s` | 4.465 | 4.546 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/blake3-readback.upload_body_mbit_s` | 4.174 | 4.239 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/sha256-store.drain_s` | 8.627 | 8.513 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/sha256-store.mib_s` | 8.116 | 8.225 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/sha256-store.of_cap` | 0.477 | 0.483 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/sha256-store.rx_mbit_s` | 0.099 | 0.096 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/sha256-store.tx_mbit_s` | 5.062 | 5.117 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap10/sha256-store.upload_body_mbit_s` | 4.768 | 4.832 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/blake3-readback.drain_s` | 26.424 | 26.449 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/blake3-readback.mib_s` | 2.65 | 2.647 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/blake3-readback.of_cap` | 0.778 | 0.777 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/blake3-readback.rx_mbit_s` | 1.542 | 1.543 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/blake3-readback.tx_mbit_s` | 1.675 | 1.688 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/blake3-readback.upload_body_mbit_s` | 1.557 | 1.555 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/sha256-store.drain_s` | 25.023 | 25.056 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/sha256-store.mib_s` | 2.798 | 2.794 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/sha256-store.of_cap` | 0.822 | 0.821 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/sha256-store.rx_mbit_s` | 0.048 | 0.047 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/sha256-store.tx_mbit_s` | 1.77 | 1.765 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.cap2/sha256-store.upload_body_mbit_s` | 1.644 | 1.642 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/blake3-readback.calibration` | [8.062, 8.565] | [8.246, 8.801] |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/blake3-readback.drain_s` | 14.03 | 14.079 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/blake3-readback.mib_s` | 4.99 | 4.973 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/blake3-readback.of_cap` | 0.293 | 0.292 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/blake3-readback.rx_mbit_s` | 3.202 | 3.205 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/blake3-readback.tx_mbit_s` | 3.203 | 3.2 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/blake3-readback.upload_body_mbit_s` | 2.932 | 2.922 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/sha256-store.calibration` | [8.062, 8.565] | [8.246, 8.801] |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/sha256-store.drain_s` | 8.674 | 8.836 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/sha256-store.mib_s` | 8.072 | 7.924 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/sha256-store.of_cap` | 0.474 | 0.466 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/sha256-store.rx_mbit_s` | 0.136 | 0.128 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/sha256-store.tx_mbit_s` | 5.023 | 4.936 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc10/sha256-store.upload_body_mbit_s` | 4.742 | 4.656 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/blake3-readback.calibration` | [1.895, 1.913] | [1.895, 1.914] |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/blake3-readback.drain_s` | 47.361 | 47.375 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/blake3-readback.rx_mbit_s` | 0.945 | 0.944 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/blake3-readback.tx_mbit_s` | 0.948 | 0.954 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/blake3-readback.upload_body_mbit_s` | 0.869 | 0.868 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/sha256-store.calibration` | [1.895, 1.913] | [1.895, 1.914] |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/sha256-store.drain_s` | 25.161 | 25.098 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/sha256-store.mib_s` | 2.783 | 2.79 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/sha256-store.of_cap` | 0.818 | 0.82 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/sha256-store.rx_mbit_s` | 0.05 | 0.051 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/sha256-store.tx_mbit_s` | 1.738 | 1.749 |
| changed | `rq5_wan.regions.eu-west-1.bandwidth.tc2/sha256-store.upload_body_mbit_s` | 1.635 | 1.639 |
| changed | `rq5_wan.regions.eu-west-1.commit` | "0abf11696e47f4db871bd4c2cda882756c114390" | "be5b419ed0658e217284e8324caaff31f39622c5" |
| changed | `rq5_wan.regions.eu-west-1.head_bucket_ms` | 71.86 | 77.59 |
| changed | `rq5_wan.regions.eu-west-1.loss.jitter.drain_s` | 7.821 | 7.832 |
| changed | `rq5_wan.regions.eu-west-1.loss.jitter.mib_s` | 8.952 | 8.94 |
| changed | `rq5_wan.regions.eu-west-1.loss.loss1.drain_s` | 16.653 | 13.403 |
| changed | `rq5_wan.regions.eu-west-1.loss.loss1.mib_s` | 4.204 | 5.224 |
| changed | `rq5_wan.regions.eu-west-1.loss.loss1.x_native` | 2.72 | 2.19 |
| changed | `rq5_wan.regions.eu-west-1.loss.loss5.drain_s` | 58.374 | 48.619 |
| changed | `rq5_wan.regions.eu-west-1.loss.loss5.mib_s` | 1.199 | 1.44 |
| changed | `rq5_wan.regions.eu-west-1.loss.loss5.x_native` | 9.54 | 7.95 |
| changed | `rq5_wan.regions.eu-west-1.native.blake3-readback.cpu_s` | 0.797 | 0.767 |
| changed | `rq5_wan.regions.eu-west-1.native.blake3-readback.drain_s` | 7.95 | 7.716 |
| changed | `rq5_wan.regions.eu-west-1.native.blake3-readback.mib_s` | [8.8095, 8.5515, 9.0585] | [9.0745, 8.803, 9.225] |
| changed | `rq5_wan.regions.eu-west-1.native.blake3-readback.tx_mbit_s` | 5.557 | 5.745 |
| changed | `rq5_wan.regions.eu-west-1.native.blake3-readback.vs_size_only_pct` | [-23.8361, -27.0079, -20.9426] | [-22.9963, -26.6495, -19.3391] |
| changed | `rq5_wan.regions.eu-west-1.native.blake3-readback.vs_us_east_1_pct` | [-51.5589, -53.4417, -49.7001] | [-51.8262, -53.3937, -50.8548] |
| changed | `rq5_wan.regions.eu-west-1.native.sha256-store.cpu_s` | 0.757 | 0.755 |
| changed | `rq5_wan.regions.eu-west-1.native.sha256-store.drain_s` | 6.117 | 6.118 |
| changed | `rq5_wan.regions.eu-west-1.native.sha256-store.mib_s` | [11.447, 11.0505, 11.6275] | [11.4445, 11.2285, 11.67] |
| changed | `rq5_wan.regions.eu-west-1.native.sha256-store.tx_mbit_s` | 7.159 | 7.155 |
| changed | `rq5_wan.regions.eu-west-1.native.sha256-store.vs_size_only_pct` | [-1.0332, -4.7697, 1.4499] | [-2.8851, -7.1217, 1.69] |
| changed | `rq5_wan.regions.eu-west-1.native.sha256-store.vs_us_east_1_pct` | [-43.1346, -45.0197, -40.3508] | [-44.2385, -45.2433, -42.9555] |
| changed | `rq5_wan.regions.eu-west-1.native.size-only.cpu_s` | 0.769 | 0.762 |
| changed | `rq5_wan.regions.eu-west-1.native.size-only.drain_s` | 6.053 | 5.941 |
| changed | `rq5_wan.regions.eu-west-1.native.size-only.mib_s` | [11.5665, 11.305, 11.875] | [11.7845, 11.288, 12.299] |
| changed | `rq5_wan.regions.eu-west-1.native.size-only.tx_mbit_s` | 7.197 | 7.297 |
| changed | `rq5_wan.regions.eu-west-1.native.size-only.vs_us_east_1_pct` | [-41.6526, -44.1839, -39.4355] | [-43.2892, -45.7907, -40.6906] |
| changed | `rq5_wan.regions.eu-west-1.native_put_get_mbit_s` | [25.92, 33.469] | [17.579, 23.704] |
| changed | `rq5_wan.regions.eu-west-1.partition.drain_after_restore_s` | 6.735 | 6.531 |
| changed | `rq5_wan.regions.eu-west-1.partition.first_commit_after_restore_s` | 2.013 | 2.031 |
| changed | `rq5_wan.regions.eu-west-1.partition.tx_resumed_after_restore_s` | 1.047 | 0.877 |
| changed | `rq5_wan.regions.eu-west-1.policed.policed.drain_s` | 391.7851 | 361.1974 |
| changed | `rq5_wan.regions.eu-west-1.policed.policed.seal_to_commit_max_s` | 198.232 | 189.397 |
| changed | `rq5_wan.regions.eu-west-1.policed.policed.tx_mbit_s` | 0.723 | 0.781 |
| changed | `rq5_wan.regions.eu-west-1.policed.policed-fixed.tx_mbit_s` | 0.889 | 0.894 |
| changed | `rq5_wan.regions.eu-west-1.rtt_ms_end` | 67.43 | 66.93 |
| changed | `rq5_wan.regions.eu-west-1.rtt_ms_per_run_median` | 66.435 | 66.325 |
| changed | `rq5_wan.regions.eu-west-1.rtt_ms_start` | 66.47 | 67.33 |
| changed | `rq5_wan.regions.eu-west-1.sigkill.blake3-readback.restart_drain_s` | 7.879 | 7.594 |
| changed | `rq5_wan.regions.eu-west-1.sigkill.sha256-store.restart_drain_s` | 6.34 | 6.44 |
| changed | `rq5_wan.regions.eu-west-1.vtop` | "0.10.2" | "0.10.4" |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/blake3-readback.drain_s` | 11.975 | 12.047 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/blake3-readback.mib_s` | 5.847 | 5.812 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/blake3-readback.of_cap` | 0.344 | 0.342 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/blake3-readback.rx_mbit_s` | 3.699 | 3.327 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/blake3-readback.tx_mbit_s` | 3.708 | 3.677 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/blake3-readback.upload_body_mbit_s` | 3.435 | 3.415 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/sha256-store.drain_s` | 9.999 | 9.736 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/sha256-store.mib_s` | 7.002 | 7.192 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/sha256-store.of_cap` | 0.411 | 0.422 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/sha256-store.rx_mbit_s` | 0.092 | 0.096 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/sha256-store.tx_mbit_s` | 4.396 | 4.502 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap10/sha256-store.upload_body_mbit_s` | 4.114 | 4.225 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/blake3-readback.drain_s` | 29.03 | 28.546 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/blake3-readback.mib_s` | 2.412 | 2.453 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/blake3-readback.of_cap` | 0.709 | 0.721 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/blake3-readback.rx_mbit_s` | 1.401 | 1.55 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/blake3-readback.tx_mbit_s` | 1.537 | 1.557 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/blake3-readback.upload_body_mbit_s` | 1.417 | 1.441 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/sha256-store.drain_s` | 26.421 | 26.501 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/sha256-store.mib_s` | 2.65 | 2.642 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/sha256-store.of_cap` | 0.778 | 0.776 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/sha256-store.rx_mbit_s` | 0.056 | 0.057 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/sha256-store.tx_mbit_s` | 1.673 | 1.668 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.cap2/sha256-store.upload_body_mbit_s` | 1.557 | 1.552 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/blake3-readback.calibration` | [7.03, 7.676] | [7.154, 6.972] |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/blake3-readback.drain_s` | 16.979 | 17.073 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/blake3-readback.mib_s` | 4.124 | 4.101 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/blake3-readback.of_cap` | 0.242 | 0.241 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/blake3-readback.rx_mbit_s` | 2.745 | 2.686 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/blake3-readback.tx_mbit_s` | 2.654 | 2.665 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/blake3-readback.upload_body_mbit_s` | 2.423 | 2.409 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/sha256-store.calibration` | [7.03, 7.676] | [7.154, 6.972] |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/sha256-store.drain_s` | 10.462 | 10.373 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/sha256-store.mib_s` | 6.692 | 6.75 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/sha256-store.of_cap` | 0.393 | 0.397 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/sha256-store.rx_mbit_s` | 0.115 | 0.11 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/sha256-store.tx_mbit_s` | 4.193 | 4.198 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc10/sha256-store.upload_body_mbit_s` | 3.932 | 3.966 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/blake3-readback.calibration` | [1.852, 1.883] | [1.852, 1.885] |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/blake3-readback.drain_s` | 49.67 | 49.391 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/blake3-readback.mib_s` | 1.41 | 1.418 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/blake3-readback.of_cap` | 0.414 | 0.416 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/blake3-readback.rx_mbit_s` | 0.917 | 0.915 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/blake3-readback.tx_mbit_s` | 0.912 | 0.908 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/blake3-readback.upload_body_mbit_s` | 0.828 | 0.833 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/sha256-store.calibration` | [1.852, 1.883] | [1.852, 1.885] |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/sha256-store.drain_s` | 26.757 | 26.738 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/sha256-store.mib_s` | 2.617 | 2.619 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/sha256-store.of_cap` | 0.768 | 0.769 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/sha256-store.tx_mbit_s` | 1.63 | 1.633 |
| changed | `rq5_wan.regions.sa-east-1.bandwidth.tc2/sha256-store.upload_body_mbit_s` | 1.537 | 1.538 |
| changed | `rq5_wan.regions.sa-east-1.commit` | "0abf11696e47f4db871bd4c2cda882756c114390" | "be5b419ed0658e217284e8324caaff31f39622c5" |
| changed | `rq5_wan.regions.sa-east-1.head_bucket_ms` | 117.63 | 117.73 |
| changed | `rq5_wan.regions.sa-east-1.loss.jitter.drain_s` | 9.899 | 9.239 |
| changed | `rq5_wan.regions.sa-east-1.loss.jitter.mib_s` | 7.073 | 7.578 |
| changed | `rq5_wan.regions.sa-east-1.loss.jitter.x_native` | 1.16 | 1.14 |
| changed | `rq5_wan.regions.sa-east-1.loss.loss1.drain_s` | 29.491 | 25.002 |
| changed | `rq5_wan.regions.sa-east-1.loss.loss1.mib_s` | 2.374 | 2.8 |
| changed | `rq5_wan.regions.sa-east-1.loss.loss1.x_native` | 3.47 | 3.09 |
| changed | `rq5_wan.regions.sa-east-1.loss.loss5.drain_s` | 78.736 | 77.887 |
| changed | `rq5_wan.regions.sa-east-1.loss.loss5.mib_s` | 0.889 | 0.899 |
| changed | `rq5_wan.regions.sa-east-1.loss.loss5.x_native` | 9.26 | 9.64 |
| changed | `rq5_wan.regions.sa-east-1.native.blake3-readback.cpu_s` | 0.849 | 0.826 |
| changed | `rq5_wan.regions.sa-east-1.native.blake3-readback.drain_s` | 10.489 | 11.005 |
| changed | `rq5_wan.regions.sa-east-1.native.blake3-readback.mib_s` | [6.675, 6.376, 6.7365] | [6.3615, 6.234, 6.623] |
| changed | `rq5_wan.regions.sa-east-1.native.blake3-readback.tx_mbit_s` | 4.201 | 4.027 |
| changed | `rq5_wan.regions.sa-east-1.native.blake3-readback.vs_size_only_pct` | [-30.2544, -33.5141, -29.0477] | [-33.8962, -35.5048, -31.1773] |
| changed | `rq5_wan.regions.sa-east-1.native.blake3-readback.vs_us_east_1_pct` | [-63.2959, -65.0665, -62.3544] | [-66.2287, -67.0641, -64.8378] |
| changed | `rq5_wan.regions.sa-east-1.native.sha256-store.cpu_s` | 0.783 | 0.773 |
| changed | `rq5_wan.regions.sa-east-1.native.sha256-store.drain_s` | 8.5 | 8.081 |
| changed | `rq5_wan.regions.sa-east-1.native.sha256-store.mib_s` | [8.2365, 8.0945, 8.449] | [8.6645, 8.35, 8.678] |
| changed | `rq5_wan.regions.sa-east-1.native.sha256-store.tx_mbit_s` | 5.165 | 5.396 |
| changed | `rq5_wan.regions.sa-east-1.native.sha256-store.vs_size_only_pct` | [-13.9387, -15.4017, -11.283] | [-9.9652, -13.2513, -9.6892] |
| changed | `rq5_wan.regions.sa-east-1.native.sha256-store.vs_us_east_1_pct` | [-59.0835, -59.8665, -57.0131] | [-57.7836, -59.4886, -57.3194] |
| changed | `rq5_wan.regions.sa-east-1.native.size-only.cpu_s` | 0.769 | 0.754 |
| changed | `rq5_wan.regions.sa-east-1.native.size-only.drain_s` | 7.316 | 7.275 |
| changed | `rq5_wan.regions.sa-east-1.native.size-only.mib_s` | [9.5705, 9.404, 9.668] | [9.6235, 9.587, 9.6715] |
| changed | `rq5_wan.regions.sa-east-1.native.size-only.tx_mbit_s` | 5.971 | 5.972 |
| changed | `rq5_wan.regions.sa-east-1.native.size-only.vs_us_east_1_pct` | [-51.7214, -53.7269, -50.1684] | [-53.6886, -54.1811, -53.2266] |
| changed | `rq5_wan.regions.sa-east-1.native_put_get_mbit_s` | [12.086, 18.206] | [15.978, 20.6] |
| changed | `rq5_wan.regions.sa-east-1.partition.drain_after_restore_s` | 8.964 | 8.816 |
| changed | `rq5_wan.regions.sa-east-1.partition.first_commit_after_restore_s` | 2.993 | 2.947 |
| changed | `rq5_wan.regions.sa-east-1.partition.tx_resumed_after_restore_s` | 1.318 | 1.357 |
| changed | `rq5_wan.regions.sa-east-1.policed.policed.drain_s` | 459.4876 | 446.6084 |
| changed | `rq5_wan.regions.sa-east-1.policed.policed.seal_to_commit_max_s` | 229.776 | 223.814 |
| changed | `rq5_wan.regions.sa-east-1.policed.policed.tx_mbit_s` | 0.617 | 0.634 |
| changed | `rq5_wan.regions.sa-east-1.policed.policed-fixed.tx_mbit_s` | 0.795 | 0.797 |
| changed | `rq5_wan.regions.sa-east-1.rtt_ms_end` | 110.8 | 110.87 |
| changed | `rq5_wan.regions.sa-east-1.rtt_ms_per_run_median` | 110.745 | 110.785 |
| changed | `rq5_wan.regions.sa-east-1.rtt_ms_start` | 110.67 | 110.5 |
| changed | `rq5_wan.regions.sa-east-1.sigkill.blake3-readback.restart_drain_s` | 10.838 | 11.628 |
| changed | `rq5_wan.regions.sa-east-1.sigkill.sha256-store.restart_drain_s` | 8.407 | 8.095 |
| changed | `rq5_wan.regions.sa-east-1.vtop` | "0.10.2" | "0.10.4" |
| expected | `vtop_commit` | "0abf116" | "be5b419" |
