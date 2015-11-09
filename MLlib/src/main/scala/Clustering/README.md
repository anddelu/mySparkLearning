创建kmeans++，算法可参考如下：文章链接：http://coolshell.cn/articles/7779.html 和 https://en.wikipedia.org/wiki/K-means%2B%2B  
我在这里重点说一下 K-Means++算法步骤：  
1：先从我们的数据库随机挑个随机点当“种子点”。  
2：对于每个点，我们都计算其和最近的一个“种子点”的距离D(x)并保存在一个数组里，然后把这些距离加起来得到Sum(D(x))。  
3：然后，再取一个随机值，用权重的方式来取计算下一个“种子点”。这个算法的实现是，先取一个能落在Sum(D(x))中的随机值Random，然后用Random -= D(x)，直到其<=0，此时的点就是下一个“种子点”。  
4：重复第（2）和第（3）步直到所有的K个种子点都被选出来。  
5：进行K-Means算法。  
python算法实现的链接：  
http://rosettacode.org/wiki/K-means%2B%2B_clustering#Python
