
$A_i=\frac{\sum(y_j)}{y_{total}}$ 为第i个箱的label为1的和与整个样本集中y=1的和的比值

$B_i=\frac{\sum((1-y)_j)}{(1-y)_{total}}$ 为第i个箱的label为0的和与整个样本集中y=0的和的比值

### woe
$$ WOE = logA - logB $$
### iv：
$$ iv = \sum(A - B)*(logA - logB) $$

对每个Feature的每个bin生成r1, r2
1. 对每一个分箱添加混淆箱，假设原本分了n个箱，增加k个混淆箱，使得分箱总数变为n+k个，其中每一个箱的结果从guest端看，均无差别。
2. 发送$A*r1$, $B*r2$ 其中，添加混淆bin，，然后发送给Guest
3. Guest解密后计算, $log(A*r1)-log(B*r2)$,然后加密后，发送回给host
4. Host选取已知的真实的箱，然后分别计算$log(A*r1/B*r2) - log(r1/r2)=woe$,
然后对每个箱的woe增加随机数干扰（但不影响其变化趋势）：$woe' = \frac{log(A/B)+r3}{r4}$ 然后发送回给guest
5. Guest解密，得到woe趋势

# iv：
$$ iv = \sum(A - B)*(logA - logB) $$

如果guest作弊，将label编码，当host将$\sum(y_i)$发回给guest时，guest即可推出host具体哪条样本在哪个bin之中，从而利用woe反解host数据。

解决思路，不让guest得到具体每个箱中的$\sum(y)$
$<w_a>_1$
具体步骤：
1. Guest依然将label加密后发给host
2. Host利用加密label统计出[[A]], [[B]] (后续直接用A，B表示)
3. Host对每个箱生成三个随机数r1, r2, r3，然后发送以下三个量给Guest
    1. $A*r1$
    2. $B*r2$
    3. $(A-B)*r3$
4. 以上假如某个特征有N个箱，增加K个混淆箱，总共N+K个箱发送给Guest

5. Guest解密每个箱后，对每个箱计算以下式子，然后加密后发回给Host
$$式子1 = (A-B)*r3*(log(A*r1) - log(B*r2))$$
5. 以上式子进行推导：
$$(A-B)*r3*(log(A*r1) - log(B*r2))=$$
$$(A-B)*r3*(logA-logB+(log(r1)-log(r2)))=$$
$$(A-B)*r3*(logA-logB) +(A-B)*r3*(log(r1) - log(r2))$$
对每个箱求和，其中Host已知某些箱是混淆箱，去掉其中的混淆箱：
$$\sum(A-B)*r3*(logA-logB) +\sum(A-B)*r3*(log(r1) - log(r2))=$$
$$r3*iv +\sum(A-B)*r3*(log(r1) - log(r2))$$
$$\therefore iv=（\sum(式子1)-\sum(A-B)*r3*(log(r1) - log(r2))）/r3$$
其中A-B密文host已知
6. 发送结果给guest，解密得到明文iv

