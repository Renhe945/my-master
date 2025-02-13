-- 开启spark的精度保护:
set spark.sql.decimalOperations.allowPrecisionLoss=false;
-- 此脚本用于计算保费信息:
-- 1- 先生成维度表信息(19338种)
-- 性别:
create or replace  view insurance_dw.prem_src0_sex as
select stack(2,'M','F') as sex;

-- 缴费期: 10 15 20 30
create or replace  view insurance_dw.prem_src0_ppp as
select stack(4,10,15,20,30) as ppp;

-- 投保年龄: 18~60
create or replace  view insurance_dw.prem_src0_age_buy as
select explode(sequence(18,60)) as age_buy;

-- 保单年度:
create or replace  view insurance_dw.prem_src0_policy_year as
select explode(sequence(1,88)) as policy_year;

-- 构建一个常量标准数据表:
create or replace view  insurance_dw.input as
select 0.035  interest_rate,    --预定利息率(Interest Rate PREM&RSV)
       0.055  interest_rate_cv,--现金价值预定利息率（Interest Rate CV）
       0.0004 acci_qx,--意外身故死亡发生率(Accident_qx)
       0.115  rdr,--风险贴现率（Risk Discount Rate)
       10000  sa,--基本保险金额(Baisc Sum Assured)
       1      average_size,--平均规模(Average Size)
       1      MortRatio_Prem_0,--Mort Ratio(PREM)
       1      MortRatio_RSV_0,--Mort Ratio(RSV)
       1      MortRatio_CV_0,--Mort Ratio(CV)
       1      CI_RATIO,--CI Ratio
       6      B_time1_B,--生存金给付时间(1)—begain
       59     B_time1_T,--生存金给付时间(1)-terminate
       0.1    B_ratio_1,--生存金给付比例(1)
       60     B_time2_B,--生存金给付时间(2)-begain
       106    B_time2_T,--生存金给付时间(2)-terminate
       0.1    B_ratio_2,--生存金给付比例(2)
       70     MB_TIME,--祝寿金给付时间
       0.2    MB_Ration,--祝寿金给付比例
       0.7    RB_Per,--可分配盈余分配给客户的比例
       0.7    TB_Per,--未分配盈余分配给客户的比例
       1      Disability_Ratio,--残疾给付保险金保额倍数
       0.1    Nursing_Ratio,--长期护理保险金保额倍数
       75     Nursing_Age--长期护理保险金给付期满年龄
;


-- 组装四个维度:
create or replace  view  insurance_dw.prem_src0 as
select
   t3.age_buy,
   t5.Nursing_Age,
   t1.sex,
   t5.B_time2_T as t_age,
   t2.ppp,
   t5.B_time2_T - t3.age_buy as bpp,
   t5.interest_rate,
   t5.sa,
   t4.policy_year,
   (t3.age_buy +  t4.policy_year) - 1 as age
from  insurance_dw.prem_src0_sex t1 join insurance_dw.prem_src0_ppp t2 on 1=1
    join insurance_dw.prem_src0_age_buy t3 on t3.age_buy >= 18 and t3.age_buy <= 70 - t2.ppp
    join insurance_dw.prem_src0_policy_year t4 on t4.policy_year >=1 and t4.policy_year <= 106 - t3.age_buy
    join insurance_dw.input as t5 on 1=1;

-- 校验维度表
select * from insurance_dw.prem_src0;


-- 步骤一: 计算 ppp_ 和 bpp_
create or replace  view  insurance_dw.prem_src1 as
select
    *,
    if( policy_year <= ppp ,1,0 ) as ppp_,
    if( policy_year <= bpp ,1,0 ) as bpp_
from insurance_dw.prem_src0;

-- 校验步骤一:
select * from insurance_dw.prem_src1 where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤二: qx  kx  和 qx_ci
create or replace view  insurance_dw.prem_src2 as
select
    t1.*,
    cast(
          if(
              t1.age <= 105,
              if(t1.sex = 'M', t3.cl1, t3.cl2),
              0
          ) * t2.MortRatio_Prem_0 * t1.bpp_
    as decimal(17,12))as qx,

    if(
        t1.age <= 105,
        if(t1.sex = 'M',t4.k_male,t4.k_female),
        0
    ) * t1.bpp_ as kx,

    if(t1.sex ='M',t4.male,t4.female) * t1.bpp_ as qx_ci

from insurance_dw.prem_src1 t1 join insurance_dw.input t2 on 1=1
    left join insurance_ods.mort_10_13 t3 on t1.age = t3.age
    left join insurance_ods.dd_table t4 on t1.age = t4.age;

-- 校验操作:
select  * from insurance_dw.prem_src2 where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤三: 计算 qx_d
create or replace  view insurance_dw.prem_src3 as
select
    *,
    cast(
        if(age = 105, qx - qx_ci, qx * (1-kx)  ) * bpp_
    as decimal(17,12)) as qx_d

from insurance_dw.prem_src2;

-- 校验操作:
select  * from insurance_dw.prem_src3 where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤四: lx
create or replace  view  insurance_dw.prem_src4_1 as
select
    *,
    if(policy_year = 1,1,NULL) as lx
from insurance_dw.prem_src3 ;

-- 校验步骤4_1
select  * from insurance_dw.prem_src4_1 where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤4_2
--  通过对 ppp(缴费期) sex age_buy(投保年龄) 分组, 即可将每组中对应的保单年度放置在一组内, 进行计算操作
drop table if exists insurance_dw.prem_src4_2;
create table if not exists insurance_dw.prem_src4_2 as
select
    age_buy,
    Nursing_Age,
    sex,
    t_age,
    ppp,
    bpp,
    interest_rate,
    sa,
    policy_year,
    age,
    ppp_,
    bpp_,
    qx,
    kx,
    qx_ci,
    qx_d,
    udaf_lx(lx,qx) over(partition by ppp,sex,age_buy order by policy_year) as lx
from insurance_dw.prem_src4_1;

-- 校验操作:
select  * from insurance_dw.prem_src4_2  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤五: 计算 lx_d dx_d dx_ci:
-- 先计算保单年度为1的时候:
create or replace  view insurance_dw.prem_src5_1 as
select
    *,
    if(policy_year = 1,1,NULL) as lx_d,
    if(policy_year = 1,qx_d,NULL) as dx_d,
    if(policy_year = 1,qx_ci,NULL) as dx_ci
from insurance_dw.prem_src4_2 ;

-- 校验:
select  * from insurance_dw.prem_src5_1  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 接着实现保单年度大于1的计算: 需要自定义UDAF函数:
drop table if exists insurance_dw.prem_src5_2;
create table if not exists insurance_dw.prem_src5_2 as
select
    age_buy,
    Nursing_Age,
    sex,
    t_age,
    ppp,
    bpp,
    interest_rate,
    sa,
    policy_year,
    age,
    ppp_,
    bpp_,
    qx,
    kx,
    qx_ci,
    qx_d,
    lx,
    udaf_3col(lx_d,qx_d,qx_ci) over(partition by  ppp,sex,age_buy order by policy_year) as lx_d_dx_d_dx_ci
from insurance_dw.prem_src5_1;

-- 校验:
select  * from insurance_dw.prem_src5_2  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 最后: 将三列合并数据切割开, 形成三列结果数据
create or replace  view insurance_dw.prem_src5_3 as
select
    age_buy,
    Nursing_Age,
    sex,
    t_age,
    ppp,
    bpp,
    interest_rate,
    sa,
    policy_year,
    age,
    ppp_,
    bpp_,
    qx,
    kx,
    qx_ci,
    qx_d,
    lx,
    cast( split(lx_d_dx_d_dx_ci,',')[0] as decimal(17,12)) as lx_d,
    cast( split(lx_d_dx_d_dx_ci,',')[1] as decimal(17,12)) as dx_d,
    cast( split(lx_d_dx_d_dx_ci,',')[2] as decimal(17,12)) as dx_ci
from insurance_dw.prem_src5_2 ;

-- 校验:
select  * from insurance_dw.prem_src5_3  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤六: 计算 cx
create or replace  view  insurance_dw.prem_src6 as
select
    *,
    dx_d / pow((1+interest_rate) , (age+1)) as cx
from insurance_dw.prem_src5_3 ;

-- 校验:
select  * from insurance_dw.prem_src6  where age_buy = 25 and sex = 'M' and ppp = 20;


-- 步骤七: 计算 cx_  和 ci_cx:
create or replace  view  insurance_dw.prem_src7 as
select
    *,
    cx * pow((1+interest_rate),0.5) as cx_,
    dx_ci / pow((1+interest_rate),(age+1)) as ci_cx
from insurance_dw.prem_src6;
-- 校验:
select  * from insurance_dw.prem_src7  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤八: ci_cx_  dx  dx_d_
create or replace  view  insurance_dw.prem_src8 as
select
    *,
    ci_cx * pow((1+interest_rate) ,0.5) as ci_cx_,
    lx / pow((1+interest_rate),age) as dx,
    lx_d / pow((1+interest_rate),age) as dx_d_

from insurance_dw.prem_src7 ;

-- 校验:
select  * from insurance_dw.prem_src8  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤九:  expense  DB1  db2_factor
create or replace  view  insurance_dw.prem_src9 as
select
    t1.*,
    case
        when t1.policy_year = 1 then  t2.r1
        when t1.policy_year = 2 then  t2.r2
        when t1.policy_year = 3 then  t2.r3
        when t1.policy_year = 4 then  t2.r4
        when t1.policy_year = 5 then  t2.r5
        else t2.r6_
    end  * t1.ppp_ as expense,

    t3.Disability_Ratio * t1.bpp_ as db1,

    if( t1.age < t1.Nursing_Age,1,0 ) * t3.Nursing_Ratio as  db2_factor

from insurance_dw.prem_src8 t1  left  join  insurance_ods.pre_add_exp_ratio t2 on t1.ppp = t2.ppp left join insurance_dw.input t3 on 1=1;

-- 校验:
select  * from insurance_dw.prem_src9  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤 10:  计算DB2  DB3  DB4  DB5
create or replace  view  insurance_dw.prem_src10 as
SELECT
    t1.*,
    sum(t1.dx * t1.db2_factor) over(partition by t1.ppp,t1.sex,t1.age_buy order by t1.policy_year rows between current row  and unbounded following)
    /t1.dx as db2,

    if(t1.age >= t1.Nursing_Age,1,0) * t2.Nursing_Ratio as db3,

    least(t1.ppp,t1.policy_year) as db4,

    (
        ifnull(sum(t1.dx * t1.ppp_) over(partition by ppp,sex,age_buy order by policy_year rows between 1 following and unbounded  following),0)
        / t1.dx
    ) * pow((1+t1.interest_rate),0.5) as db5

FROM insurance_dw.prem_src9 t1 join insurance_dw.input t2 on 1=1 ;

-- 校验:
select  * from insurance_dw.prem_src10  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 将保费参数因子的数据灌入到目标表:
insert overwrite table  insurance_dw.prem_src
select
    age_buy,
    nursing_age,
    sex,
    t_age,
    ppp,
    bpp,
    interest_rate,
    sa,
    policy_year,
    age,
    qx,
    kx,
    qx_d,
    qx_ci,
    dx_d,
    dx_ci,
    lx,
    lx_d,
    cx,
    cx_,
    ci_cx,
    ci_cx_,
    dx,
    dx_d_,
    ppp_,
    bpp_,
    expense,
    db1,
    db2_factor,
    db2,
    db3,
    db4,
    db5
from insurance_dw.prem_src10;

-- 校验:
select  count(1) from insurance_dw.prem_src ;
select  * from insurance_dw.prem_src  where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤 11: 计算保费前的中间结果值: 先分组, 然后进行统计
create or replace view insurance_dw.prem_std_src11 as
select
    ppp,sex,age_buy,
    sum(
        if(
            policy_year = 1,
            0.5 *ci_cx_ * db1 * pow((1+interest_rate),-0.25),
            ci_cx_ * db1
        )
    )  as  t11,

    sum(
       if(
            policy_year = 1,
            0.5 * ci_cx_ * db2 * pow((1+interest_rate),-0.25),
            ci_cx_ * db2
        )
    ) as v11,

    sum(dx * db3) as w11,

    sum(dx * ppp_) as q11,

    sum(
        if(
            policy_year = 1,
            0.5 * ci_cx_ * pow((1+interest_rate),0.25),
            0
        )
    ) as t9,

    sum(
        if(
            policy_year = 1,
            0.5 * ci_cx_ * pow((1+interest_rate),0.25),
            0
        )
    ) as v9,

    sum(dx * expense) as s11,

    sum(cx_ * db4) as x11,

    sum(ci_cx_ * db5) as y11
from insurance_dw.prem_src
group by ppp,sex,age_buy;

-- 校验:
select  * from  insurance_dw.prem_std_src11 where age_buy = 25 and sex = 'M' and ppp = 20;

-- 步骤十二: 核算保费:
create or replace view  insurance_dw.prem_std_src12 as
select
    t1.age_buy,
    t1.sex,
    t1.ppp,
    106-t1.age_buy as bpp,
    input.sa * (t1.t11 + t1.v11 + t1.w11) / (t1.q11 -t1.t9 - t1.v9 -t1.s11 - t1.x11 - t1.y11) as prem
from insurance_dw.prem_std_src11 t1 join insurance_dw.input on 1=1 ;

-- 校验:
select  * from insurance_dw.prem_std_src12 where age_buy = 50 and sex = 'M' and ppp = 20;

-- 保存到目标表
insert  overwrite  table  insurance_dw.prem_std
select
    age_buy,
    sex,
    ppp,
    bpp,
    prem
from  insurance_dw.prem_std_src12;

-- 校验数据
select count(1) from insurance_dw.prem_std;
select *  from insurance_dw.prem_std  where age_buy = 50 and sex = 'M' and ppp = 20;
