import math

LEFT_MOST_POINT = -800000
RIGHT_MOST_POINT = 800000

# sqrt of 1.0001^(-800000) in 2^96 power
MIN_PRICE = 337263108622
# sqrt of 1.0001^(800000) in 2^96 power
MAX_PRICE = 18611883644907511909590774894315720731532604461

def pow_128():
   return 1<<128

def pow_96():
   return 1<<96

def sqrt_rate_96():
   return get_sqrt_price(1)


def mul_fraction_floor(number, _numerator, _denominator):
   return number * _numerator // _denominator
   
def mul_fraction_ceil(number, _numerator, _denominator):
   res = number * _numerator // _denominator
   if (number * _numerator % _denominator == 0):
      return res
   else:
      return (res+1)

# sqrt(1.0001^point)
# from https://github.com/izumiFinance/izumi-swap-core/blob/main/contracts/libraries/LogPowMath.sol#L16-L44
# compute the price at a given point
# @param point: the point
# @return the price of the point
def get_sqrt_price(point: int):
   if point > RIGHT_MOST_POINT or point < LEFT_MOST_POINT:
      print("E202_ILLEGAL_POINT")
      return None

   abs_point = point
   if point < 0:
      abs_point = -point

   value = 0x100000000000000000000000000000000
   if point & 1 != 0:
      value = 0xfffcb933bd6fad37aa2d162d1a594001

   value = update_value(abs_point, value, 0x2, 0xfff97272373d413259a46990580e213a)
   value = update_value(abs_point, value, 0x4, 0xfff2e50f5f656932ef12357cf3c7fdcc)
   value = update_value(abs_point, value, 0x8, 0xffe5caca7e10e4e61c3624eaa0941cd0)
   value = update_value(abs_point, value, 0x10, 0xffcb9843d60f6159c9db58835c926644)
   value = update_value(abs_point, value, 0x20, 0xff973b41fa98c081472e6896dfb254c0)
   value = update_value(abs_point, value, 0x40, 0xff2ea16466c96a3843ec78b326b52861)
   value = update_value(abs_point, value, 0x80, 0xfe5dee046a99a2a811c461f1969c3053)
   value = update_value(abs_point, value, 0x100, 0xfcbe86c7900a88aedcffc83b479aa3a4)
   value = update_value(abs_point, value, 0x200, 0xf987a7253ac413176f2b074cf7815e54)
   value = update_value(abs_point, value, 0x400, 0xf3392b0822b70005940c7a398e4b70f3)
   value = update_value(abs_point, value, 0x800, 0xe7159475a2c29b7443b29c7fa6e889d9)
   value = update_value(abs_point, value, 0x1000, 0xd097f3bdfd2022b8845ad8f792aa5825)
   value = update_value(abs_point, value, 0x2000, 0xa9f746462d870fdf8a65dc1f90e061e5)
   value = update_value(abs_point, value, 0x4000, 0x70d869a156d2a1b890bb3df62baf32f7)
   value = update_value(abs_point, value, 0x8000, 0x31be135f97d08fd981231505542fcfa6)
   value = update_value(abs_point, value, 0x10000, 0x9aa508b5b7a84e1c677de54f3e99bc9)
   value = update_value(abs_point, value, 0x20000, 0x5d6af8dedb81196699c329225ee604)
   value = update_value(abs_point, value, 0x40000, 0x2216e584f5fa1ea926041bedfe98)
   value = update_value(abs_point, value, 0x80000, 0x48a170391f7dc42444e8fa2)

   if point > 0:
      value = ((1 << 256) - 1) // value

   remainder = 0
   if value % (1 << 32):
      remainder = 1
   return (value >> 32)+remainder


def update_value(point, value, hex1, hex2):
   if point & hex1 != 0:
      value = value * hex2
      value = (value >> 128)
   return value


# floor(log1.0001(sqrtPrice_96))
#def get_log_sqrt_price_floor( sqrt_price_96: float ):
#   return int(math.log(1.0001,sqrt_price_96))


# from https://github.com/izumiFinance/izumi-swap-core/blob/main/contracts/libraries/LogPowMath.sol#L47-L190
# compute the point at a given price
# @param sqrt_price_96: the price.
# @return the point of the price
def get_log_sqrt_price_floor(sqrt_price_96: int):
   if( sqrt_price_96 < MIN_PRICE or sqrt_price_96 > MAX_PRICE):
      print("E201_INVALID_SQRT_PRICE")
      return None
   
   sqrt_price_128  = sqrt_price_96 << 32

   x = sqrt_price_128
   m = 0

   (x, m) = update_x_m(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, 7, x, m)
   (x, m) = update_x_m(0xFFFFFFFFFFFFFFFF, 6, x, m)
   (x, m) = update_x_m(0xFFFFFFFF, 5, x, m)
   (x, m) = update_x_m(0xFFFF, 4, x, m)
   (x, m) = update_x_m(0xFF, 3, x, m)
   (x, m) = update_x_m(0xF, 2, x, m)
   (x, m) = update_x_m(0x3, 1, x, m)

   y = 0
   if x > 1:
      y = 1

   m |= y

   if m >= 128:
      x = sqrt_price_128 >> (m - 127)
   else:
      x = sqrt_price_128 << (127 - m)

   l2 = (m - 128) << 64

   (x, l2) = update_x_l2(63, x, l2)
   (x, l2) = update_x_l2(62, x, l2)
   (x, l2) = update_x_l2(61, x, l2)
   (x, l2) = update_x_l2(60, x, l2)
   (x, l2) = update_x_l2(59, x, l2)
   (x, l2) = update_x_l2(58, x, l2)
   (x, l2) = update_x_l2(57, x, l2)
   (x, l2) = update_x_l2(56, x, l2)
   (x, l2) = update_x_l2(55, x, l2)
   (x, l2) = update_x_l2(54, x, l2)
   (x, l2) = update_x_l2(53, x, l2)
   (x, l2) = update_x_l2(52, x, l2)
   (x, l2) = update_x_l2(51, x, l2)

   x = x * x
   x = (x >> 127)
   y = (x >> 128)
   l2 = (l2 | (y << 50))

   ls10001 = l2 * 255738958999603826347141
   log_floor = (ls10001 - 3402992956809132418596140100660247210) >> 128
   log_upper = (ls10001 + 291339464771989622907027621153398088495) >> 128
   
   if log_floor == log_upper:
      return log_floor
   elif get_sqrt_price(log_upper) <= sqrt_price_96:
      return log_upper
   else:
      return log_floor


def update_x_m(hex1, offset, x, m):
   y = 0
   if(x > hex1):
      y = (1 << offset)
   m = (m | y)
   x = (x >> y)
   return (x, m)
   
def update_x_l2(offset, x, l2):
   x = x * x
   x = (x >> 127)
   y = (x >> 128)
   l2 = (l2 | (y << offset))
   x = (x >> y)
   return (x,l2)

# Get amount of token Y that is needed to add a unit of liquidity in the range [left_pt, right_pt)
# @param sqrt_price_l_96: sqrt of left point price in 2^96 power
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param sqrt_rate_96: sqrt of 1.0001 in 2^96 power
# @return amount of token Y
# get_amount_y_unit_liquidity_96( sqrt_price_l_96: U256, sqrt_price_r_96: U256,sqrt_rate_96: U256) -> U256
def get_amount_y_unit_liquidity_96( sqrt_price_l_96: int, sqrt_price_r_96: int,sqrt_rate_96: int):
   numerator = sqrt_price_r_96 - sqrt_price_l_96
   denominator = sqrt_rate_96 - pow_96()
   return mul_fraction_ceil(pow_96(), numerator, denominator)


# Get amount of token X that is needed to add a unit of liquidity in the range [left_pt, right_pt)
# @param left_pt: left point of this range
# @param right_pt: right point of this range
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param sqrt_rate_96: sqrt of 1.0001 in 2^96 power
# @return amount of token X
# get_amount_x_unit_liquidity_96( left_pt: i32, right_pt: i32, sqrt_price_r_96: U256, sqrt_rate_96: U256) -> U256
def get_amount_x_unit_liquidity_96( left_pt: int, right_pt: int, sqrt_price_r_96: int, sqrt_rate_96: int):
   sqrt_price_pr_pc_96 = get_sqrt_price(right_pt - left_pt + 1)
   sqrt_price_pr_pd_96 = get_sqrt_price(right_pt + 1)

   numerator = sqrt_price_pr_pc_96 - sqrt_rate_96
   denominator = sqrt_price_pr_pd_96 - sqrt_price_r_96
   return mul_fraction_ceil(pow_96(), numerator, denominator)


# Get amount of token Y that can form liquidity in range [l, r)
# @param liquidity: L = Y/sqrt(p)
# @param sqrt_price_l_96: sqrt of left point price in 2^96 power
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param sqrt_rate_96: sqrt of 1.0001 in 2^96 power
# @param upper: flag to indicate rounding up or down
# @return amount of token Y that can from given liquidity
# get_amount_y( liquidity: u128, sqrt_price_l_96: U256, sqrt_price_r_96: U256, sqrt_rate_96: U256, upper: bool) -> U256
def get_amount_y( liquidity: int, sqrt_price_l_96: int, sqrt_price_r_96: int, sqrt_rate_96: int, upper: bool):
   # d = 1.0001, ∵ L = Y / sqrt(P)   ∴ Y(i) = L * sqrt(d ^ i)
   # sqrt(d) ^ r - sqrt(d) ^ l
   # ------------------------- = amount_y_of_unit_liquidity: the amount of token Y equivalent to a unit of liquidity in the range
   # sqrt(d) - 1
   #
   # sqrt(d) ^ l * sqrt(d) ^ (r - l) - sqrt(d) ^ l
   # ----------------------------------------------
   # sqrt(d) - 1
   # 
   # sqrt(d) ^ l * (sqrt(d) ^ (r - l) - 1)
   # ----------------------------------------------
   # sqrt(d) - 1
   #
   # sqrt(d) ^ l * (sqrt(d) - 1) * (sqrt(d) ^ (r - l - 1) + sqrt(d) ^ (r - l - 2) + ...... + sqrt(d) + 1)
   # ----------------------------------------------------------------------------------------------------
   # sqrt(d) - 1
   # 
   # sqrt(d) ^ l + sqrt(d) ^ (l + 1) + ...... + sqrt(d) ^ (r - 1) 
   # 
   # Y(l) + Y(l + 1) + ...... + Y(r - 1) 

   # amount_y = amount_y_of_unit_liquidity * liquidity

   # using sum equation of geomitric series to compute range numbers
   numerator = sqrt_price_r_96 - sqrt_price_l_96
   denominator = sqrt_rate_96 - pow_96()
   if upper == False:
      return mul_fraction_floor(liquidity, numerator, denominator)
   else:
      return mul_fraction_ceil(liquidity, numerator, denominator)


# Get amount of token X that can form liquidity in range [l, r)
# @param liquidity: L = X*sqrt(p)
# @param left_pt: left point of this range
# @param right_pt: right point of this range
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param sqrt_rate_96: sqrt of 1.0001 in 2^96 power
# @param upper: flag to indicate rounding up or down
# @return amount of token X that can from given liquidity
# get_amount_x( liquidity: u128, left_pt: i32, right_pt: i32, sqrt_price_r_96: U256, sqrt_rate_96: U256, upper: bool) -> U256
def get_amount_x( liquidity: int, left_pt: int, right_pt: int, sqrt_price_r_96: int, sqrt_rate_96: int, upper: bool):
   # d = 1.0001,  ∵ L = X * sqrt(P)   ∴ X(i) = L / sqrt(d ^ i)
   # sqrt(d) ^ (r - l) - 1
   # --------------------------------- = amount_x_of_unit_liquidity: the amount of token X equivalent to a unit of  c in the range
   # sqrt(d) ^ r - sqrt(d) ^ (r - 1)
   # 
   # (sqrt(d) - 1) * (sqrt(d) ^ (r - l - 1) + sqrt(d) ^ (r - l - 2) + ...... + 1)
   # ----------------------------------------------------------------------------
   # (sqrt(d) - 1) * sqrt(d) ^ (r - 1))
   #
   #      1                1                             1
   # ------------ + ----------------- + ...... + -----------------
   # sqrt(d) ^ l    sqrt(d) ^ (l + 1)            sqrt(d) ^ (r - 1)
   #
   # X(l) + X(l + 1) + ...... + X(r - 1)

   # amount_x = amount_x_of_unit_liquidity * liquidity

   sqrt_price_pr_pl_96 = get_sqrt_price(right_pt - left_pt)
   sqrt_price_pr_m1_96 = mul_fraction_floor(sqrt_price_r_96, pow_96(), sqrt_rate_96)

   # using sum equation of geomitric series to compute range numbers
   numerator = sqrt_price_pr_pl_96 - pow_96()
   denominator = sqrt_price_r_96 - sqrt_price_pr_m1_96
   if upper == False:
      return mul_fraction_floor(liquidity, numerator, denominator)
   else:
      return mul_fraction_ceil(liquidity, numerator, denominator)

# compute the most left point so that all liquidities in [most_left_point, right_pt) would be swapped out by amount_x
# @param liquidity: liquidity in each point
# @param amount_x: the amount of token X used in swap
# @param right_pt: right point of this range
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @return the most left point in this range swap, if it equals to right_pt, means nothing swapped in this range
# get_most_left_point( liquidity: u128, amount_x: u128, right_pt: i32, sqrt_price_r_96: U256) -> i32
def get_most_left_point( liquidity: int, amount_x: int, right_pt: int, sqrt_price_r_96: int):
   # d = 1.0001
   # sqrt(d) ^ (r - l) - 1
   # --------------------------------- * liquidity = amount_x
   # sqrt(d) ^ r - sqrt(d) ^ (r - 1)
   #
   # sqrt(d) ^ (r - l) = amount_x * (sqrt(d) ^ r - sqrt(d) ^ (r - 1)) / liquidity + 1

   sqrt_price_pr_m1_96 = mul_fraction_ceil(sqrt_price_r_96, pow_96(), sqrt_rate_96())
   sqrt_value_96 = mul_fraction_floor(amount_x, sqrt_price_r_96 - sqrt_price_pr_m1_96, liquidity) + pow_96()
   log_value = get_log_sqrt_price_floor(sqrt_value_96)
   return (right_pt - log_value)

# compute the most right point so that all liquidities in [left_point, most_right_point) would be swapped out by amount_y
# @param liquidity: liquidity in each point
# @param amount_y: the amount of token Y used in swap
# @param sqrt_price_l_96: sqrt of left point price in 2^96 power
# @return the most right point in this range swap, if it equals to left_pt, means nothing swapped in this range
# get_most_right_point(liquidity: u128, amount_y: u128, sqrt_price_l_96: U256) -> i32
def get_most_right_point(liquidity: int, amount_y: int, sqrt_price_l_96: int):
   # d = 1.0001
   # sqrt(d) ^ r - sqrt(d) ^ l
   # ------------------------- * liquidity = amount_y
   # sqrt(d) - 1
   #
   # sqrt(d) ^ r - sqrt(d) ^ l = amount_y * (sqrt(d) - 1) / liquidity

   sqrt_loc_96 = mul_fraction_floor(amount_y, sqrt_rate_96() - pow_96(), liquidity) + sqrt_price_l_96
   return get_log_sqrt_price_floor(sqrt_loc_96)

# Compute the token X and token Y that need to be added to add the specified liquidity in the specified range
# @param left_point: left point of this range
# @param right_point: right point of this range
# @param liquidity: The amount of liquidity expected to be added
# @return (amount_x, amount_y)
def compute_deposit_x_y( liquidity, left_point, right_point, current_point ):
   user_liquidity_y = 0
   user_liquidity_x = 0
   sqrt_price_96 = get_sqrt_price(current_point)
   sqrt_price_r_96 = get_sqrt_price(right_point)
   if left_point < current_point:
      sqrt_price_l_96 = get_sqrt_price(left_point)
      if right_point < current_point:
          user_liquidity_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), True)
      else:
          user_liquidity_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_96, sqrt_rate_96(), True)

   if right_point > current_point:
      xr_left = 0
      if left_point > current_point:
         xr_left = left_point
      else:
         xr_left = current_point + 1

      user_liquidity_x = get_amount_x(liquidity, xr_left, right_point, sqrt_price_r_96, sqrt_rate_96(), True)
      
   if left_point <= current_point and right_point > current_point:
      user_liquidity_y += mul_fraction_ceil(liquidity, sqrt_price_96, pow_96())
      
   return ( user_liquidity_x, user_liquidity_y )

# Compute the token X and token Y obtained by removing the specified liquidity in the specified range
# @param liquidity: The amount of liquidity expected to be removed
# @param left_point: left point of this range
# @param right_point: right point of this range
# @return (amount_x, amount_y)
def compute_withdraw_x_y( liquidity, left_point, right_point, current_point, pool_liquidity, pool_liquidity_x ):
   sqrt_price_96 = get_sqrt_price(current_point)
   sqrt_price_r_96 = get_sqrt_price(right_point)
   
   amount_y = 0
   if left_point < current_point:
      sqrt_price_l_96 = get_sqrt_price(left_point)
      if right_point < current_point:
         amount_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), False)
      else:
         amount_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_96, sqrt_rate_96(), False)

   amount_x = 0
   if right_point > current_point:
      xr_left = current_point + 1
      if left_point > current_point:
         xr_left = left_point
      amount_x = get_amount_x(liquidity, xr_left, right_point, sqrt_price_r_96, sqrt_rate_96(), False)

   if left_point <= current_point and right_point > current_point:
      withdrawed_liquidity_y = pool_liquidity - pool_liquidity_x # pool当前point的liquidity_y
      if withdrawed_liquidity_y >= liquidity:
         withdrawed_liquidity_y = liquidity
      withdrawed_liquidity_x = liquidity - withdrawed_liquidity_y
      amount_y += mul_fraction_floor(withdrawed_liquidity_y, sqrt_price_96, pow_96());
      amount_x += mul_fraction_floor(withdrawed_liquidity_x, pow_96(), sqrt_price_96);
      #print("liquidity =",liquidity,", withdrawed_liquidity_x =",withdrawed_liquidity_x)
      pool_liquidity -= liquidity
      pool_liquidity_x -= withdrawed_liquidity_x
   return (amount_x, amount_y, pool_liquidity, pool_liquidity_x)



if __name__ == "__main__":
   '''
   get_sqrt_price(0) = 79228162514264337593543950336
   get_sqrt_price(1) = 79232123823359799118286999568
   get_sqrt_price(-1) = 79224201403219477170569942574
   get_sqrt_price(799999) = 18610953120514014497639399516106032187649727623
   get_sqrt_price(-800000) = 337263108622
   '''
   a = get_sqrt_price(0)
   b = get_sqrt_price(1)
   c = get_sqrt_price(-1)
   d = get_sqrt_price(799999)
   e = get_sqrt_price(-800000)
   
   if a != 79228162514264337593543950336:
      print("Error1")
   else:
      print("Pass1")
   if b != 79232123823359799118286999568:
      print("Error2")
   else:
      print("Pass2")
   if c != 79224201403219477170569942574:
      print("Error3")
   else:
      print("Pass3")  
   if d != 18610953120514014497639399516106032187649727623:
      print("Error4")
   else:
      print("Pass4")
   if e != 337263108622:
      print("Error5")
   else:
      print("Pass5")    
      
   if 0 != get_log_sqrt_price_floor(a):
      print("Error6")
   else:
      print("Pass6")
   if 1 != get_log_sqrt_price_floor(b):
      print("Error7")
   else:
      print("Pass7")
   if -1 != get_log_sqrt_price_floor(c):
      print("Error8")
   else:
      print("Pass8")
   if 799999 != get_log_sqrt_price_floor(d):
      print("Error9")
   else:
      print("Pass9")
   if -800000 != get_log_sqrt_price_floor(e):
      print("Error10")
   else:
      print("Pass10")
