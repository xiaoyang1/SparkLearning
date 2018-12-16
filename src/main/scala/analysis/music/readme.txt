这个包主要是用来学习spark高级数据分析的 音乐推荐的 例子的。

背景：
从经验上来讲， 推荐引擎大体上属于大规模机器学习。大家对此都了解，而且大部分人在
亚马逊上都见过。 从社交网络到视频网站，再到在线零售，都用到了推荐引擎，大家都知
道推荐引擎。 实际应用中的推荐引擎我们也能直接看到。虽然我们知道 Spotify 上是计算
机在挑选播放的歌曲，

1. 数据集 Audioscrobbler 数据集。 这是last.fm 的第一个音乐推荐你系统。
在 last.fm 的年代，推荐引擎方面的研究大多局限于评分类数据。换句话说，人们常常把推
荐引擎看成处理“Bob 给 Prince 的评价是 3 星半”这类输入数据的工具。

这个数据集有些特别： 它只记录了播放数据，如“Bob 播放了一首 Prince的歌曲”。播放记录所包含的信息比评分要少。
http://www.iro.umontreal.ca/~lisa/datasets/profiledata_06-May-2005.tar.gz
下载归档文件后，你会发现里面有几个文件。 主要的数据集在文件 user_artist_data.txt 中，它包含 141 000 个用
户和 160 万个艺术家，记录了约 2420 万条用户播放艺术家歌曲的信息，其中包括播放次数信息。

数据集在 artist_data.txt 文件中给出了每个艺术家的 ID 和对应的名字。请注意，记录播放
信息时，客户端应用提交的是艺术家的名字。名字如果有拼写错误，或使用了非标准的名
称， 事后才能被发现。 比如，“The Smiths”“Smiths, The”和“the smiths”看似代表不同
艺术家的 ID，但它们其实明显是指同一个艺术家。因此，为了将拼写错误的艺术家 ID 或
ID 变体对应到该艺术家的规范 ID，数据集提供了 artist_alias.txt 文件。
user_artist_data.txt
    3 columns: userid artistid playcount

artist_data.txt
    2 columns: artistid artist_name

artist_alias.txt
    2 columns: badid, goodid
    known incorrectly spelt artists and the correct artist id.
    you can correct errors in user_artist_data as you read it in using this file
    (we're not yet finished merging this data)

我们采用交替最小二乘推荐算法。





