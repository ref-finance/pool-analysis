from dcl_math import *

# group returned values of x2YRange to avoid stake too deep
class X2YRangeRet:
   def __init__(self):
      # whether user run out of amountX
      self.finished = False
      # actual cost of tokenX to buy tokenY
      self.cost_x = 0
      # amount of acquired tokenY
      self.acquire_y = 0
      # final point after this swap
      self.final_pt = 0
      # sqrt price on final point
      self.sqrt_final_price_96 = 0
      # liquidity of tokenX at finalPt
      self.liquidity_x = 0

class X2YRangeRetDesire:
   def __init__(self):
      self.finished = False
      self.cost_x = 0
      self.acquire_y = 0
      self.final_pt = 0
      self.sqrt_final_price_96 = 0
      self.liquidity_x = 0

class Y2XRangeRetDesire:
   def __init__(self):
      self.finished = False
      self.cost_y = 0
      self.acquire_x = 0
      self.final_pt = 0
      self.sqrt_final_price_96 = 0
      self.liquidity_x = 0

class X2YRangeCompRet:
   def __init__(self):
      self.cost_x = 0
      self.acquire_y = 0
      self.complete_liquidity = False
      self.loc_pt = 0
      self.sqrt_loc_96 = 0

class Y2XRangeRet:
   def __init__(self):
      # whether user has run out of token_y
      self.finished = False
      # actual cost of token_y to buy token_x
      self.cost_y = 0
      # actual amount of token_x acquired
      self.acquire_x = 0
      # final point after this swap
      self.final_pt = 0
      # sqrt price on final point
      self.sqrt_final_price_96 = 0
      # liquidity of token_x at final_pt
      # if final_pt is not right_pt, liquidity_x is meaningless
      self.liquidity_x = 0

class Y2XRangeCompRet:
   def __init__(self):
      self.cost_y = 0
      self.acquire_x = 0
      self.complete_liquidity = False
      self.loc_pt = 0
      self.sqrt_loc_96 = 0

class X2YRangeCompRetDesire:
   def __init__(self):
      self.cost_x = 0
      self.acquire_y = 0
      self.complete_liquidity = False
      self.loc_pt = 0
      self.sqrt_loc_96 = 0

class Y2XRangeCompRetDesire:
   def __init__(self):
      self.cost_y = 0
      self.acquire_x = 0
      self.complete_liquidity = False
      self.loc_pt = 0
      self.sqrt_loc_96 = 0



# @param amount_x: the amount of swap-in token X
# @param sqrt_price_96: price of this point
# @param liquidity: liquidity amount on this point
# @param liquidity_x: liquidity part from X*sqrt(p)
# @return tuple (consumed_x, swap_out_y, new_liquidity_x)
def x_swap_y_at_price_liquidity(amount_x: int, sqrt_price_96: int, liquidity: int,liquidity_x: int):
   liquidity_y = liquidity - liquidity_x
   max_transform_liquidity_x = mul_fraction_floor(amount_x, sqrt_price_96, pow_96())
   transform_liquidity_x = min(max_transform_liquidity_x, liquidity_y)
   
   # rounding up to ensure pool won't be short of X.
   cost_x = mul_fraction_ceil(transform_liquidity_x, pow_96(), sqrt_price_96)
   # TODO: convert to u128
   # rounding down to ensure pool won't be short of Y.
   acquire_y = mul_fraction_floor(transform_liquidity_x, sqrt_price_96, pow_96())
   new_liquidity_x = liquidity_x + transform_liquidity_x
   return  (cost_x, acquire_y, new_liquidity_x)

# @param amount_y: the amount of swap-in token Y
# @param sqrt_price_96: price of this point
# @param liquidity_x: liquidity part from X*sqrt(p)
# @return tuple (consumed_y, swap_out_x, new_liquidity_x)
def y_swap_x_at_price_liquidity(amount_y: int, sqrt_price_96: int, liquidity_x: int):
   max_transform_liquidity_y = mul_fraction_floor(amount_y, pow_96(), sqrt_price_96)
   transform_liquidity_y = min(max_transform_liquidity_y, liquidity_x)
   cost_y = mul_fraction_ceil(transform_liquidity_y,sqrt_price_96, pow_96())
   acquire_x = mul_fraction_floor(transform_liquidity_y,pow_96(), sqrt_price_96)
   new_liquidity_x = liquidity_x - transform_liquidity_y
   return (cost_y, acquire_x, new_liquidity_x)

# try to swap from right to left in range [left_point, right_point) with all liquidity used.
# @param liquidity: liquidity of each point in the range
# @param sqrt_price_l_96: sqrt of left point price in 2^96 power
# @param left_point: left point of this range
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param right_point: right point of this range
# @param amount_x: amount of token X as swap-in
# @return X2YRangeCompRet
#     .complete_liquidity, True if given range has been fully swapped
#     .cost_x, used amount of token X
#     .acquire_y, acquired amount of token Y
#     .loc_pt, if partial swapped, the right most unswapped point
#     .sqrt_loc_96, the sqrt_price of loc_pt
def x_swap_y_range_complete(liquidity: int, sqrt_price_l_96: int, left_point: int, sqrt_price_r_96: int, right_point: int, amount_x: int):
   result = X2YRangeCompRet()

   max_x = get_amount_x(liquidity, left_point, right_point, sqrt_price_r_96, sqrt_rate_96(), True)
      
   if max_x <= amount_x:
      # liquidity in this range has been FULLY swapped out
      result.complete_liquidity = True
      result.cost_x = max_x
      result.acquire_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), False)
   else:
      # liquidity in this range can only be PARTIAL swapped out
      result.complete_liquidity = False
      result.loc_pt = get_most_left_point(liquidity, amount_x, right_point, sqrt_price_r_96)
      # the distance between left and point must be non-negative
      if result.loc_pt > right_point:
         print("E208_INTERNAL_ERR1")
         return None

      # it would be fully swap if violated
      if result.loc_pt <= left_point:
         print("E209_INTERNAL_ERR2")
         return None
      
      if result.loc_pt == right_point:
         # could not exhaust one point liquidity
         result.cost_x = 0
         result.acquire_y = 0
      else:
         # exhaust some point liquidity but not all point
         cost_x_256 = get_amount_x(liquidity, result.loc_pt, right_point, sqrt_price_r_96, sqrt_rate_96(), True)            
         result.cost_x = min(cost_x_256, amount_x)
         result.acquire_y = get_amount_y(liquidity, get_sqrt_price(result.loc_pt), sqrt_price_r_96, sqrt_rate_96(), False)

      # put current point to the right_point - 1 to wait for single point process
      result.loc_pt -= 1
      result.sqrt_loc_96 = get_sqrt_price(result.loc_pt)

   return result

# try to swap from left to right in range [left_point, right_point) with all liquidity used.
# @param liquidity: liquidity of each point in the range
# @param sqrt_price_l_96: sqrt of left point price in 2^96 power
# @param left_point: left point of this range
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param right_point: right point of this range
# @param amount_y: amount of token Y as swap-in
# @return Y2XRangeCompRet
#     .complete_liquidity, True if given range has been fully swapped
#     .cost_y, used amount of token Y
#     .acquire_x, acquired amount of token X
#     .loc_pt, if partial swapped, the right most unswapped point
#     .sqrt_loc_96, the sqrt_price of loc_pt   
def y_swap_x_range_complete(liquidity: int, sqrt_price_l_96: int, left_point: int, sqrt_price_r_96: int, right_point: int,  amount_y: int):
   result = Y2XRangeCompRet()
   max_y = get_amount_y( liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), True)
   if max_y <= amount_y :
      result.cost_y = max_y
      result.acquire_x = get_amount_x( liquidity, left_point, right_point, sqrt_price_r_96, sqrt_rate_96(), False )
      result.complete_liquidity = True
   else:
      result.loc_pt = get_most_right_point(liquidity, amount_y, sqrt_price_l_96)

      # the distance between right and point must be non-negative
      if result.loc_pt < left_point:
         print("E210_INTERNAL_ERR3")
         return None

      # it would be fully swap if violated
      if result.loc_pt >= right_point:
         print("E211_INTERNAL_ERR4")
         return None

      result.complete_liquidity = False
      result.sqrt_loc_96 = get_sqrt_price(result.loc_pt)
      if result.loc_pt == left_point:
         result.cost_y = 0
         result.acquire_x = 0
         return result

      cost_y_256 = get_amount_y( liquidity, sqrt_price_l_96, result.sqrt_loc_96, sqrt_rate_96(), True )

      result.cost_y = min(cost_y_256, amount_y)

      result.acquire_x = get_amount_x( liquidity, left_point, result.loc_pt, result.sqrt_loc_96, sqrt_rate_96(), False )

   return result

# @param amount_x: the amount of swap-in token X
# @param sqrt_price_96: price of this point
# @param curr_y: the amount of token Y that can participate in the calc
# @return tuple (cost_x, acquire_y)
# x_swap_y_at_price( amount_x: u128, sqrt_price_96: U256, curr_y: u128) -> (u128, u128)
def x_swap_y_at_price( amount_x: int, sqrt_price_96: int, curr_y: int):
   l = mul_fraction_floor(amount_x, sqrt_price_96, pow_96())

   acquire_y = mul_fraction_floor(l, sqrt_price_96, pow_96())
   if acquire_y > curr_y:
      acquire_y = curr_y

   l = mul_fraction_ceil(acquire_y, pow_96(), sqrt_price_96)
   cost_x = mul_fraction_ceil(l, pow_96(), sqrt_price_96)
   return (cost_x, acquire_y)


# @param amount_y: the amount of swap-in token Y
# @param sqrt_price_96: price of this point
# @param curr_x: the amount of token X that can participate in the calc
# @return tuple (cost_y, acquire_x)
# y_swap_x_at_price( amount_y: u128, sqrt_price_96: U256,  curr_x: u128) -> (u128, u128)
def y_swap_x_at_price( amount_y: int, sqrt_price_96: int,  curr_x: int):
   l = mul_fraction_floor(amount_y, pow_96(), sqrt_price_96)
   acquire_x = min(mul_fraction_floor(l,pow_96(), sqrt_price_96), curr_x )
   l = mul_fraction_ceil(acquire_x, sqrt_price_96, pow_96())
   cost_y = mul_fraction_ceil(l, sqrt_price_96, pow_96())
   return (cost_y, acquire_x)


# @param desire_y: the amount of swap-out token Y
# @param sqrt_price_96: price of this point
# @param liquidity: liquidity of each point in the range
# @param liquidity_x: liquidity part from X*sqrt(p)
# @return tuple (cost_x, acquire_y, new_liquidity_x)
# x_swap_y_at_price_liquidity_desire( desire_y: u128, sqrt_price_96: U256, liquidity: u128, liquidity_x: u128) -> (U256, u128, u128)
def x_swap_y_at_price_liquidity_desire( desire_y: int, sqrt_price_96: int, liquidity: int, liquidity_x: int):
   liquidity_y = liquidity - liquidity_x
   max_transform_liquidity_x = mul_fraction_ceil(desire_y, pow_96(), sqrt_price_96)
   transform_liquidity_x = min(max_transform_liquidity_x, liquidity_y)
   cost_x = mul_fraction_ceil(transform_liquidity_x, pow_96(), sqrt_price_96)
   acquire_y = mul_fraction_floor(transform_liquidity_x, sqrt_price_96, pow_96())
   new_liquidity_x = liquidity_x + transform_liquidity_x
   return (cost_x, acquire_y, new_liquidity_x)


# @param desire_x: the amount of swap-out token X
# @param sqrt_price_96: price of this point
# @param liquidity_x: liquidity part from X*sqrt(p)
# @return tuple (cost_y, acquire_x, new_liquidity_x)
# y_swap_x_at_price_liquidity_desire( desire_x: u128, sqrt_price_96: U256, liquidity_x: u128 )
def y_swap_x_at_price_liquidity_desire( desire_x: int, sqrt_price_96: int, liquidity_x: int ):
   max_transform_liquidity_y = mul_fraction_ceil(desire_x, sqrt_price_96, pow_96())
   # transformLiquidityY <= liquidityX <= uint128.max
   transform_liquidity_y = min(max_transform_liquidity_y, liquidity_x)
   cost_y = mul_fraction_ceil(transform_liquidity_y, sqrt_price_96, pow_96())
   acquire_x = mul_fraction_floor(transform_liquidity_y, pow_96(), sqrt_price_96)
   new_liquidity_x = liquidity_x - transform_liquidity_y
   return (cost_y, acquire_x, new_liquidity_x)


# try to swap from left to right in range [left_point, right_point) with all liquidity used.
# @param liquidity: liquidity of each point in the range
# @param sqrt_price_l_96: sqrt of left point price in 2^96 power
# @param left_point: left point of this range
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param right_point: right point of this range
# @param desire_y: amount of token Y as swap-out
# @return X2YRangeCompRetDesire
# x_swap_y_range_complete_desire( liquidity: u128, sqrt_price_l_96: U256, left_point: i32, sqrt_price_r_96: U256, right_point: i32,  desire_y: u128) -> X2YRangeCompRetDesire
def x_swap_y_range_complete_desire( liquidity: int, sqrt_price_l_96: int, left_point: int, sqrt_price_r_96: int, right_point: int,  desire_y: int):
   result = X2YRangeCompRetDesire()
   max_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), False)
   if max_y <= desire_y:
      result.acquire_y = max_y
      result.cost_x = get_amount_x(liquidity, left_point, right_point, sqrt_price_r_96, sqrt_rate_96(), True)
      result.complete_liquidity = True
      return result
   
   cl = sqrt_price_r_96 - mul_fraction_floor(desire_y, sqrt_rate_96() - pow_96(), liquidity)
   
   result.loc_pt = get_log_sqrt_price_floor(cl) + 1
   
   result.loc_pt = min(result.loc_pt, right_point)
   result.loc_pt = max(result.loc_pt, left_point + 1)
   result.complete_liquidity = False

   if result.loc_pt == right_point:
      result.cost_x = 0
      result.acquire_y = 0
      result.loc_pt -= 1
      result.sqrt_loc_96 = get_sqrt_price(result.loc_pt)
   else:
      sqrt_price_pr_mloc_96 = get_sqrt_price(right_point - result.loc_pt)
      sqrt_price_pr_m1_96 = mul_fraction_ceil(sqrt_price_r_96, pow_96(), sqrt_rate_96())
      
      result.cost_x = mul_fraction_ceil(liquidity, sqrt_price_pr_mloc_96 - pow_96(), sqrt_price_r_96 - sqrt_price_pr_m1_96)

      result.loc_pt -= 1
      result.sqrt_loc_96 = get_sqrt_price(result.loc_pt)

      sqrt_loc_a1_96 = result.sqrt_loc_96 + mul_fraction_floor(result.sqrt_loc_96, sqrt_rate_96() - pow_96(), pow_96())
      
      acquire_y = get_amount_y(liquidity, sqrt_loc_a1_96, sqrt_price_r_96, sqrt_rate_96(), False)
      result.acquire_y = min(acquire_y, desire_y)

   return result


# try to swap from right to left in range [left_point, right_point) with all liquidity used.
# @param liquidity: liquidity of each point in the range
# @param sqrt_price_l_96: sqrt of left point price in 2^96 power
# @param left_point: left point of this range
# @param sqrt_price_r_96: sqrt of right point price in 2^96 power
# @param right_point: right point of this range
# @param desire_x: amount of token X as swap-out
# @return Y2XRangeCompRetDesire
# y_swap_x_range_complete_desire( liquidity: u128, sqrt_price_l_96: U256, left_point: i32, sqrt_price_r_96: U256, right_point: i32,  desire_x: u128) -> Y2XRangeCompRetDesire
def y_swap_x_range_complete_desire( liquidity: int, sqrt_price_l_96: int, left_point: int, sqrt_price_r_96: int, right_point: int,  desire_x: int):
   result = Y2XRangeCompRetDesire()
   max_x = get_amount_x(liquidity, left_point, right_point, sqrt_price_r_96, sqrt_rate_96(), False)
   if max_x <= desire_x:
      # maxX <= desireX <= uint128.max
      result.acquire_x = max_x
      result.cost_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), True)
      result.complete_liquidity = True
      return result


   sqrt_price_pr_pl_96 = get_sqrt_price(right_point - left_point)
   sqrt_price_pr_m1_96 = mul_fraction_floor(sqrt_price_r_96, pow_96(), sqrt_rate_96())
   div = sqrt_price_pr_pl_96 - mul_fraction_floor(desire_x, sqrt_price_r_96 - sqrt_price_pr_m1_96, liquidity)

   sqrt_price_loc_96 = mul_fraction_floor(sqrt_price_r_96, pow_96(), div) # modified according to audition

   result.complete_liquidity = False
   result.loc_pt = get_log_sqrt_price_floor(sqrt_price_loc_96)

   result.loc_pt = max(left_point, result.loc_pt)
   result.loc_pt = min(right_point - 1, result.loc_pt)
   result.sqrt_loc_96 = get_sqrt_price(result.loc_pt)

   if result.loc_pt == left_point:
      result.acquire_x = 0
      result.cost_y =0
      return result

   result.complete_liquidity = False
   result.acquire_x = min( get_amount_x(liquidity, left_point, result.loc_pt, result.sqrt_loc_96, sqrt_rate_96(), False), desire_x)

   result.cost_y = get_amount_y(liquidity, sqrt_price_l_96, result.sqrt_loc_96, sqrt_rate_96(), True)
   return result


# @param desire_y: the amount of swap-out token Y
# @param sqrt_price_96: price of this point
# @param curr_y: the amount of token Y that can participate in the calc
# @return tuple (cost_x, acquire_y)
# x_swap_y_at_price_desire( desire_y: u128, sqrt_price_96: U256,curr_y: u128 )
def x_swap_y_at_price_desire( desire_y: int, sqrt_price_96: int,curr_y: int ):
   if acquire_y > curr_y:
      acquire_y = curr_y

   l = mul_fraction_ceil(acquire_y, pow_96(), sqrt_price_96)
   cost_x = mul_fraction_ceil(l, pow_96(), sqrt_price_96)
   return (cost_x, acquire_y)


# @param desire_x: the amount of swap-out token X
# @param sqrt_price_96: price of this point
# @param curr_x: the amount of token X that can participate in the calc
# @return tuple (cost_y, acquire_x)
# y_swap_x_at_price_desire( desire_x: u128, sqrt_price_96: U256, curr_x: u128) -> (u128, u128)
def y_swap_x_at_price_desire( desire_x: int, sqrt_price_96: int, curr_x: int):
   acquire_x = min(desire_x, curr_x)
   l = mul_fraction_ceil(acquire_x, sqrt_price_96, pow_96())
   cost_y = mul_fraction_ceil(l, sqrt_price_96, pow_96())
   return (cost_y, acquire_x)
