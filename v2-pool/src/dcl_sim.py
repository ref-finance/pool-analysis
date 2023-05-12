
# dcl implementation in python for aptos/near blockchain
# 1. get the whole state data from chain
# 2. get quoted price with python dcl algo version

# interface for end user:
# https://shimo.im/docs/B1Aw19jvnJsBMpqm

# 1. sync code from main branch 2.2.0 
# 2. stats data per endpoint
# 3. fetch tx from outside
# 4. replay the tx
# 5. save the stats to database

import os
import sys
import time
import datetime
import json
from dcl_swap import *
from dcl_common import *
from dcl_math import *
import copy
from aws_s3_client import get_last_two_block_height_from_all_s3_folders_list, fetch_dcl_files_from_s3
from utils import gen_info_filepath, get_ft_metadata
from config import Cfg


dcl_state_error = []

def put_error_log(error_info):
   msg = {}
   msg['error_msg'] = error_info
   dcl_state_error.append(msg)

def OpenFile(filepath):
   with open(filepath, mode='r', encoding="utf-8") as f:
      json_obj = json.load(f)
   return json_obj

def default(instance):
    return {k: v
            for k, v in vars(instance).items()
            if not str(k).startswith('_')}

############################################################################################

# Function to count the no. of leading zeros
def countLeadingZeros(x):
    # Keep shifting x by one until leftmost bit does not become 1.
    total_bits = 256
    res = 0
    while ((x & (1 << (total_bits - 1))) == 0):
        x = (x << 1)
        res += 1
    return res

def countTrailingZeros(v):
    return (v & -v).bit_length() - 1 

# return Some(0) if 0...01
# return Some(255) if 1...0
# fn idx_of_most_left_set_bit(value: U256) -> Option<u8>
def idx_of_most_left_set_bit(value: int):
   if ( value == 0 ):
      return None
   else:
      return (255 - countLeadingZeros(value))


# return Some(0) if 01...1
# return Some(255) if 10...0
# fn idx_of_most_right_set_bit(value: U256) -> Option<u8>
def idx_of_most_right_set_bit(value: int):
   if( value == 0 ):
      return None
   else:
      return countTrailingZeros(value)

class RangeInfo:
   def __init__(self):
      self.left_point = 0   # include this point
      self.right_point = 0  # exclude this point
      self.amount_l = 0     # liquidity amount of each point in this Range

class PointOrderInfo:
   def __init__(self):
      self.point = 0
      # x y would be one and only one has none zero value
      self.amount_x = 0
      self.amount_y = 0

class MarketDepth:
   def __init__(self):
      self.pool_id = ""
      self.current_point = 0
      self.amount_l = 0  # total liquidity at current point
      self.amount_l_x = 0  # liquidity caused by token X at current point
      self.liquidities = {}  # HashMap<i32, RangeInfo>, key is start point of this range
      self.orders = {} # HashMap<i32, PointOrderInfo>,  key is the order located

      
class FeeType:
   def __init__(self):
      self.point_delta = 0
      self.fee = 0

class User:
   def __init__(self):
      # A copy of an user ID. Saves one storage_read when iterating on users.
      self.user_id = ""
      self.sponsor_id = ""
      self.user_rate_idx = 0,
      self.liquidity_keys = [] # UnorderedSet<LptId>,
      #[serde(skip_serializing)]
      self.order_keys = {} # UnorderedMap<UserOrderKey, OrderId>,
      #[serde(skip_serializing)]
      self.history_orders = [] # Vector<UserOrder>,
      #[serde(with = "u64_dec_format")]
      self.completed_order_count = 0
      #[serde(skip_serializing)]
      self.assets = {} # UnorderedMap<AccountId, Balance>,
      #[serde(skip_serializing)]
      self.mft_assets = {} # UnorderedMap<MftId, Balance>,



class UserLiquidity:
   def __init__(self):
      self.LptId = ""
      self.owner_id = ""
      self.pool_id = ""
      self.left_point = 0
      self.right_point = 0
      self.last_fee_scale_x_128 = 0
      self.last_fee_scale_y_128 = 0
      self.amount = 0
      self.mft_id = ""
      self.v_liquidity = 0
      self.unclaimed_fee_x = 0
      self.unclaimed_fee_y = 0

   def is_mining(self):
      return (self.mft_id=="" or self.mft_id==None) and self.v_liquidity !=0

   # 
   # @param acc_fee_x_in_128
   # @param acc_fee_y_in_128
   def get_unclaimed_fee(self, acc_fee_x_in_128, acc_fee_y_in_128):
      # In current algorithm, left point fee_out plus right point fee_out may bigger than total fee, 
      # cause a negative value of last_fee_scale(from acc_fee_x_in), so we use overflowed sub to take U256 as I256
      self.unclaimed_fee_x = mul_fraction_floor(acc_fee_x_in_128 - self.last_fee_scale_x_128, self.amount, pow_128())
      self.unclaimed_fee_y = mul_fraction_floor(acc_fee_y_in_128 - self.last_fee_scale_y_128, self.amount, pow_128())


class LiquidityData:
   def __init__(self):
      self.liquidity_sum = 0
      self.liquidity_delta = 0
      self.acc_fee_x_out_128 = 0
      self.acc_fee_y_out_128 = 0
   
   def pass_endpoint(self, fee_scale_x_128: int, fee_scale_y_128: int):
      self.acc_fee_x_out_128 = fee_scale_x_128 - self.acc_fee_x_out_128
      self.acc_fee_y_out_128 = fee_scale_y_128 - self.acc_fee_y_out_128

   def dump(self):
      print("----------------dump LiquidityData-------------------")
      print("liquidity_sum:",self.liquidity_sum)
      print("liquidity_delta:",self.liquidity_delta)
      print("acc_fee_x_out_128:",self.acc_fee_x_out_128)
      print("acc_fee_y_out_128:",self.acc_fee_y_out_128)
      print("----------------dump LiquidityData completed-------------------")

class OrderData:
   def __init__(self):
      self.selling_x = 0
      self.earn_y = 0
      self.earn_y_legacy = 0
      self.acc_earn_y = 0
      self.acc_earn_y_legacy = 0
   
      self.selling_y = 0
      self.earn_x = 0
      self.earn_x_legacy = 0
      self.acc_earn_x = 0
      self.acc_earn_x_legacy = 0
      
      self.user_order_count = 0

   def dump(self):
      print("----------------dump OrderData-------------------")
      print("selling_x:",self.selling_x)
      print("earn_y:",self.earn_y)
      print("earn_y_legacy:",self.earn_y_legacy)
      print("acc_earn_y:",self.acc_earn_y)
      print("acc_earn_y_legacy:",self.acc_earn_y_legacy)
      print("selling_y:",self.selling_y)
      print("earn_x:",self.earn_x)
      print("earn_x_legacy:",self.earn_x_legacy)
      print("acc_earn_x:",self.acc_earn_x)
      print("acc_earn_x_legacy:",self.acc_earn_x_legacy)
      print("----------------dump OrderData completed-------------------")

class PointData:
   def __init__(self):
      #self.liquidity_data = LiquidityData()
      #self.order_data = OrderData()
      self.liquidity_data = None
      self.order_data = None

   # see if corresponding bit in slot_bitmap should be set
   def has_active_liquidity(self):
      if self.liquidity_data:
         return self.liquidity_data.liquidity_sum > 0
      return False


   # see if there is some x to sell
   def has_active_order_x(self):
     if self.order_data:
         return self.order_data.selling_x != 0
     return False

   # see if there is some y to sell
   def has_active_order_y(self):
      if self.order_data:
         return self.order_data.selling_y != 0
      return False

   # see if corresponding bit in slot_bitmap should be set
   def has_active_order(self):
      return (self.has_active_order_x() or self.has_active_order_y())

   # tell self.liquidity_data should be Some or None
   def has_liquidity(self):
      if self.liquidity_data:
         return self.liquidity_data.liquidity_sum > 0
      return False

   # tell self.order_data should be Some or None
   def has_order(self):
      if self.order_data:
         return self.order_data.user_order_count > 0
     
      return False


   ####################################

class PointStats:
   def __init__(self):
      # for user requirement.
      self.liquidity_volume_x_in = 0
      self.liquidity_volume_y_in = 0
      self.liquidity_volume_x_out = 0
      self.liquidity_volume_y_out = 0
      self.order_volume_x_in = 0
      self.order_volume_y_in = 0
      self.order_volume_x_out = 0
      self.order_volume_y_out = 0
      self.fee_x = 0
      self.fee_y = 0
      self.p_fee_x = 0
      self.p_fee_y = 0
      
   def dump(self):
      print("----------------dump PointStats-------------------")
      print("liquidity_volume_x_in:",self.volume_x_in)
      print("liquidity_volume_y_in:",self.volume_y_in)
      print("liquidity_volume_x_out:",self.volume_x_out)
      print("liquidity_volume_y_out:",self.volume_y_out)
      print("order_volume_x_in:",self.volume_x_in)
      print("order_volume_y_in:",self.volume_y_in)
      print("order_volume_x_out:",self.volume_x_out)
      print("order_volume_y_out:",self.volume_y_out)
      print("fee_x:",self.fee_x)
      print("fee_y:",self.fee_y)
      print("----------------dump PointStats completed-------------------")      
   ####################################

# 统计最后的结果，最终存在数据库中
class StatsResult:
   def __init__(self):
      self.l = 0 #liquidity
      self.tvl_x_l = 0 #token_x
      self.tvl_y_l = 0 #token_y
      self.tvl_x_o = 0 #order_x
      self.tvl_y_o = 0 #order_y
      self.vol_x_in_l = 0 #liquidity_x_in
      self.vol_y_in_l = 0 #liquidity_y_in
      self.vol_x_out_l = 0 #liquidity_x_out
      self.vol_y_out_l = 0 #liquidity_y_out
      self.vol_x_in_o = 0 #order_x_in
      self.vol_y_in_o = 0 #order_y_in
      self.vol_x_out_o = 0 #order_x_out
      self.vol_y_out_o = 0 #order_y_out
      self.fee_x = 0 # fee x for lp
      self.fee_y = 0 # fee y for lp
      self.p_fee_x = 0 # protocol fee x
      self.p_fee_y = 0 # protocol fee y
      self.p = 0  # price

class PointInfo:
   def __init__(self):
      self.data = {}
      self.stats_data = {}
   
   def dump(self):
      print("----------------dump point_info-------------------")
      for point, point_data in self.data.items():
         print("point: ", point)
         if point_data.liquidity_data:
            print("liquidity_sum = ",point_data.liquidity_data.liquidity_sum)
            print("liquidity_delta = ",point_data.liquidity_data.liquidity_delta)
            print("acc_fee_x_out_128 = ",point_data.liquidity_data.acc_fee_x_out_128)
            print("acc_fee_y_out_128 = ",point_data.liquidity_data.acc_fee_y_out_128)
         else:
            print("point_data.liquidity_data: None")
   
         if point_data.order_data:
            print("selling_x = ",point_data.order_data.selling_x)
            print("earn_y = ",point_data.order_data.earn_y)
            print("earn_y_legacy = ",point_data.order_data.earn_y_legacy)
            print("acc_earn_y = ",point_data.order_data.acc_earn_y)
            print("acc_earn_y_legacy = ",point_data.order_data.acc_earn_y_legacy)
            print("selling_y = ",point_data.order_data.selling_y)
            print("earn_x = ",point_data.order_data.earn_x)
            print("earn_x_legacy = ",point_data.order_data.earn_x_legacy)
            print("acc_earn_x = ",point_data.order_data.acc_earn_x)
            print("acc_earn_x_legacy = ",point_data.order_data.acc_earn_x_legacy)
         else:
            print("point_data.order_data: None")
         print()
      print("----------------dump point_info completed-------------------")

   # 统计所有endpoint上的liquidity, x_in, x_out, y_in, y_out, fee_x, fee_y
   def dump_stats_data(self, current_point: int, point_delta: int, pool_fee: int, protocol_fee_rate: int, x_decimal: int, y_decimal: int):
      stats_dict = {}
      # sort at first
      self.data = json.loads(json.dumps(self.data, sort_keys = True, default=default))
      
      # deal with liquidity & limit order
      acc_delta = 0
      last_point = -400001
      for point, data in self.data.items():
         # liquidity
         if last_point > -400001 and last_point < int(point):
            for pt in range(last_point+point_delta, int(point),point_delta):
               stats_dict[str(pt)] = copy.deepcopy(stats_dict[str(last_point)])

         if data['liquidity_data'] and len(data['liquidity_data']) > 0: # data['liquidity_data'] could be None and empty
            acc_delta += data['liquidity_data']['liquidity_delta']
         if acc_delta > 0:
            if point not in stats_dict:
               stats_dict[point] = StatsResult()
               stats_dict[point].p = (1.0001**int(point)) * (10**(x_decimal - y_decimal))
               
            stats_dict[point].l = acc_delta
            (stats_dict[point].tvl_x_l, stats_dict[point].tvl_y_l) = compute_deposit_x_y(acc_delta, int(point), int(point)+point_delta, current_point)
            stats_dict[point].tvl_x_l = stats_dict[point].tvl_x_l / 10**x_decimal
            stats_dict[point].tvl_y_l = stats_dict[point].tvl_y_l / 10**y_decimal
            
            last_point = int(point)
         
         # limit order
         if data['order_data']:
            if point not in stats_dict:
               stats_dict[point] = StatsResult()            
               stats_dict[point].p = (1.0001**int(point)) * (10**(x_decimal - y_decimal))

            stats_dict[point].tvl_x_o = data['order_data']['selling_x'] / 10**x_decimal
            stats_dict[point].tvl_y_o = data['order_data']['selling_y'] / 10**y_decimal
      
      # deal with token_in, token_out, fee_x, fee_y etc
      for point, data in self.stats_data.items():
         if str(point) not in stats_dict:
            stats_dict[str(point)] = StatsResult()
            stats_dict[str(point)].p = (1.0001**int(point)) * (10**(x_decimal - y_decimal))
         stats_dict[str(point)].vol_x_in_l = data.liquidity_volume_x_in / 10**x_decimal
         stats_dict[str(point)].vol_y_in_l = data.liquidity_volume_y_in / 10**y_decimal
         stats_dict[str(point)].vol_x_out_l = data.liquidity_volume_x_out / 10**x_decimal
         stats_dict[str(point)].vol_y_out_l = data.liquidity_volume_y_out / 10**y_decimal
         stats_dict[str(point)].vol_x_in_o = data.order_volume_x_in / 10**x_decimal
         stats_dict[str(point)].vol_y_in_o = data.order_volume_y_in / 10**y_decimal
         stats_dict[str(point)].vol_x_out_o = data.order_volume_x_out / 10**x_decimal
         stats_dict[str(point)].vol_y_out_o = data.order_volume_y_out / 10**y_decimal
         stats_dict[str(point)].fee_x = data.fee_x / 10**x_decimal
         stats_dict[str(point)].fee_y = data.fee_y / 10**x_decimal
         stats_dict[str(point)].p_fee_x = data.p_fee_x / 10**x_decimal
         stats_dict[str(point)].p_fee_y = data.p_fee_y / 10**x_decimal

         
      # verify the fee_x based on vol_x_in_l,vol_x_in_o and fee_y based on vol_x_in_o,vol_y_in_o
      for point, data in self.stats_data.items():
         pool_fee_x = (stats_dict[str(point)].vol_x_in_l + stats_dict[str(point)].vol_x_in_o) * pool_fee // 10**6
         pool_fee_y = (stats_dict[str(point)].vol_y_in_l + stats_dict[str(point)].vol_y_in_o) * pool_fee // 10**6
         
         protocol_fee_x = pool_fee_x * protocol_fee_rate // BP_DENOM
         protocol_fee_y = pool_fee_y * protocol_fee_rate // BP_DENOM
         
         total_fee_x = stats_dict[str(point)].fee_x + stats_dict[str(point)].p_fee_x
         total_fee_y = stats_dict[str(point)].fee_y + stats_dict[str(point)].p_fee_y
         
         if math.fabs(total_fee_x - pool_fee_x) > 1 / 10**x_decimal:
            print(point," pool_fee_x: ", str(pool_fee_x), ", total_fee_x: ", str(total_fee_x))
         if math.fabs(total_fee_y - pool_fee_y) > 1 / 10**y_decimal:
            print(point," pool_fee_y: ", pool_fee_y, ", total_fee_y: ", str(total_fee_y))

      # re-sort
      stats_dict = json.loads(json.dumps(stats_dict, sort_keys = True, default=default))
      with open("stats_dict.json", mode='w', encoding="utf-8") as f:
        json.dump(stats_dict, f, indent = 2, sort_keys = True)
      return stats_dict


      
   def load_point_info(self, point_info ):
      for key, value in point_info.items():
         point_data = PointData()
         point_data.liquidity_data = LiquidityData()
         point_data.order_data = OrderData()
         
         if len(value['liquidity_data']) > 0: # if value['liquidity_data'] is not empty
            point_data.liquidity_data.__dict__ = value['liquidity_data']
         if len(value['order_data']) > 0: # if value['order_data'] is not empty
            point_data.order_data.__dict__ = value['order_data']
         self.data[int(key)] = point_data # change the key from str to int
   
   def remove(self, point: int):
      self.data.pop(point, None)
   
   def get_point_data(self, point: int):
      if point in self.data.keys():
         return self.data[point]
      else:
         return None

   def get_point_data_or_default(self, point: int):
      if point in self.data.keys():
         return self.data[point]
      else:
         return PointData()

   def set_point_data(self, point: int, point_data):
      self.data[point] = point_data
   
   def get_liquidity_data(self, point: int):
      if point not in self.data.keys():
         self.data[point] = PointData()
         self.data[point].liquidity_data = LiquidityData()
      else:
         if self.data[point].liquidity_data is None:
            self.data[point].liquidity_data = LiquidityData()
      return self.data[point].liquidity_data

   def set_liquidity_data(self, point: int, liquidity_data):
      if point not in self.data.keys():
         self.data[point] = PointData()
      self.data[point].liquidity_data = liquidity_data

   def get_order_data(self, point: int):
      if point not in self.data.keys():
         self.data[point] = PointData()
         self.data[point].order_data = OrderData()
      else:
         if self.data[point].order_data is None:
            self.data[point].order_data = OrderData()
      return self.data[point].order_data

   def set_order_data(self, point: int, order_data):
      if point not in self.data.keys():
         self.data[point] = PointData()
      self.data[point].order_data = order_data

   def has_active_liquidity( self, point: int, point_delta: int):
      if point % point_delta == 0:
         point_data = self.get_point_data(point)
         if point_data:
            return point_data.has_active_liquidity()
      return False

   def has_active_order(self, point: int, point_delta: int):
      if point % point_delta == 0:
         point_data = self.get_point_data(point)
         if point_data:
            return point_data.has_active_order()
      return False

   def get_point_type_value(self, point: int, point_delta: int ):
      point_type = 0
      if point % point_delta == 0:
         if self.has_active_liquidity(point, point_delta):
             point_type |= 1
         if self.has_active_order(point, point_delta):
             point_type |= 2
      return point_type


   def get_fee_in_range(self, left_point: int, right_point: int, current_point: int, fee_scale_x_128: int, fee_scale_y_128: int ):
      if left_point not in self.data or right_point not in self.data:
        print("current_point =", current_point,", left_point =", left_point,", right_point =", right_point)
      
      left_point_data = self.data[left_point].liquidity_data
      right_point_data = self.data[right_point].liquidity_data
      
      fee_scale_lx_128 = get_fee_scale_l(left_point, current_point, fee_scale_x_128, left_point_data.acc_fee_x_out_128)
      fee_scale_gex_128 = get_fee_scale_ge(right_point, current_point, fee_scale_x_128, right_point_data.acc_fee_x_out_128)
      fee_scale_ly_128 = get_fee_scale_l(left_point, current_point, fee_scale_y_128, left_point_data.acc_fee_y_out_128)
      fee_scale_gey_128 = get_fee_scale_ge(right_point, current_point, fee_scale_y_128, right_point_data.acc_fee_y_out_128)
      #print("get_fee_in_range",left_point_data,right_point_data,fee_scale_lx_128,fee_scale_gex_128,fee_scale_ly_128,fee_scale_gey_128)
      #print("get_fee_in_range, left_point_data.acc_fee_x_out_128 = ",left_point_data.acc_fee_x_out_128)
      #print("get_fee_in_range, right_point_data.acc_fee_x_out_128 = ",right_point_data.acc_fee_x_out_128)
      
      return (fee_scale_x_128 - fee_scale_lx_128 - fee_scale_gex_128, fee_scale_y_128 - fee_scale_ly_128 - fee_scale_gey_128 )

   def update_endpoint( self, endpoint: int, is_left: bool, current_point: int, liquidity_delta: int, max_liquidity_per_point: int, fee_scale_x_128: int, fee_scale_y_128: int):
      point_data = self.data.pop(endpoint, PointData())
      
      liquidity_data = point_data.liquidity_data
      if liquidity_data == None:
         liquidity_data = LiquidityData()
         
      liquid_acc_before = liquidity_data.liquidity_sum
      liquid_acc_after = 0
      if liquidity_delta > 0:
         liquid_acc_after = liquid_acc_before + liquidity_delta
      else:
         liquid_acc_after = liquid_acc_before - (-liquidity_delta)
      
      if( liquid_acc_after > max_liquidity_per_point):
         print("liquid_acc_after : "+str(liquid_acc_after)+", max_liquidity_per_point : "+str(max_liquidity_per_point))
         print("Error: E203_LIQUIDITY_OVERFLOW : "+self.name)
         raise Exception("liquid_acc_after > max_liquidity_per_point")
      
      liquidity_data.liquidity_sum = liquid_acc_after

      if is_left:
         liquidity_data.liquidity_delta += liquidity_delta
      else:
         liquidity_data.liquidity_delta -= liquidity_delta

      new_or_erase = False
      if liquid_acc_before == 0:
         new_or_erase = True
         if endpoint >= current_point:
            liquidity_data.acc_fee_x_out_128 = fee_scale_x_128
            liquidity_data.acc_fee_y_out_128 = fee_scale_y_128
      elif liquid_acc_after == 0:
         new_or_erase = True

      point_data.liquidity_data = liquidity_data
      self.data[endpoint] = point_data
      #print("update_endpoint: endpoint= ",endpoint)

      return new_or_erase

   def get_point_stats_data_or_default(self, point: int):
      if point in self.stats_data.keys():
         return self.stats_data[point]
      else:
         return PointStats()

   def set_point_stats_data(self, point: int, point_stats_data):
      self.stats_data[point] = point_stats_data

def get_fee_scale_l( endpoint: int, current_point: int, fee_scale_128: int, fee_scale_beyond_128: int ): 
   if (endpoint <= current_point):
      return (fee_scale_beyond_128)
   else:
      return (fee_scale_128 - fee_scale_beyond_128)

def get_fee_scale_ge( endpoint: int, current_point: int, fee_scale_128: int, fee_scale_beyond_128: int):
   if (endpoint > current_point):
      return fee_scale_beyond_128
   else:
      return (fee_scale_128 - fee_scale_beyond_128)


class Slot_BitMap:
   def __init__(self):
      self.data = {}

   def load_slot_bitmap(self, slot_bitmap):
      for key, value in slot_bitmap.items():
         import binascii 
         self.data[int(key)] = int.from_bytes(binascii.a2b_hex(value), 'little', signed = False) # little endian

   def dump(self):
      print("----------------dump slot_bitmap-------------------")
      for key, value in self.data.items():
         print("word_idx:",key,", value:",hex(value))
      print("----------------dump slot_bitmap completed-------------------")
   
   def initialize(self, slot_bitmap_account_address: str, point_delta: int, eventHandleStruct: str, limit: int, start: int):
      event_client = EventClient( NODE_URL )
      bitmap_info = event_client.getBitMap( slot_bitmap_account_address, eventHandleStruct, limit, start )
      print(bitmap_info)
      for element in bitmap_info:
         print("sequence_number : ", element['sequence_number'], ", ",element['data'])
         point = int(element['data']['point'])
         if (point & (1 << 63)):
            point = -(point - (1 << 63))
         if( element['data']['value'] == 0):
            self.set_zero(point, point_delta)
         else:
            self.set_one(point, point_delta)

   def remove(self, point: int):
      self.data.pop(point, None)

   def set_zero(self,point: int, point_delta: int ):
      if( point % point_delta != 0 ):
         print("E200_INVALID_ENDPOINT")
         return
      map_pt = point // point_delta
      word_idx = (map_pt >> 8 )
      bit_idx = map_pt % 256
      
      val = self.data.pop(word_idx,0)
      new_val = val & (~(1<< bit_idx))
      
      if new_val != 0:
         self.data[word_idx] = new_val
      #print("set_zero: word_idx = ", word_idx, ", data[word_idx] = ",new_val)

   def set_one(self,point: int, point_delta: int ):
      if( point % point_delta != 0 ):
         print("E200_INVALID_ENDPOINT")
         return
      map_pt = point // point_delta
      word_idx = (map_pt >> 8 )
      bit_idx = map_pt % 256
      
      val = self.data.pop(word_idx,0)
      
      if( val != 0):
         self.data[word_idx] = val | (1 << bit_idx)
      else:
         self.data[word_idx] = 1 << bit_idx
      #print("set_one: point = ", point, ", map_pt = ", map_pt, ", word_idx = ", word_idx, ", data[word_idx] = ", hex(self.data[word_idx]), ", bit_idx = ", bit_idx)

   def get_bit(self, point: int, point_delta: int ):
      if( point % point_delta != 0 ):
         print("E200_INVALID_ENDPOINT")
         return
      map_pt = point // point_delta
      word_idx = (map_pt >> 8 )
      bit_idx = map_pt % 256
      
      if word_idx in self.data:
         return self.data[word_idx] & (1 << bit_idx)
      else:
         return 0
      
   def get_endpoints(self, point_delta: int ):
      endpoints = []
      for word_idx, value in self.data.items(): # take word_idx
         for bit_idx in range(0,256): # iterate the bit_idx from 0 to 255
            if value & (1 << bit_idx): # judge 
               map_pt = (word_idx << 8) + bit_idx
               point = map_pt * point_delta
               print("Slot_BitMap.get_endpoints - point =",point,",point_delta =",point_delta,",map_pt=",map_pt, ",word_idx=",word_idx,", value =",hex(value))
               endpoints.append(point)
      return endpoints

   # return start point of a valued (with liquidity or order) slot that beside the given point from left,
   # including the slot that embrace given point
   # return None if no valued slot found at the right of stop_slot (including stop_slot)
   # pub fn get_nearest_left_valued_slot( &self, point: i32, point_delta: i32, stop_slot: i32 ) -> Option<i32>
   def get_nearest_left_valued_slot(self, point: int, point_delta: int, stop_slot: int):
      slot = point // point_delta
      if ( point < 0 and point % point_delta != 0 ):
         slot -= 1 # round towards negative infinity
      word_idx = slot >> 8
      bit_idx = slot % 256
      #print("slot =",slot, ", word_idx =",word_idx, ", bit_idx =",bit_idx)
      
      slot_word = 0
      
      if( word_idx in self.data.keys()) :
         # from 0001000 to 0001111, then bitand to only remain equal&lower bits
         slot_word = self.data[word_idx] & ( ((1<<bit_idx) - 1)+(1<<bit_idx) )
         #print("self.data[word_idx] = ",self.data[word_idx])
         #print("(1<<bit_idx - 1)+1<<bit_idx = ",format(((1<<bit_idx) - 1)+(1<<bit_idx), '0256b'))
      base_slot = slot - bit_idx
      #print("slot_word = ",slot_word, ", base_slot = ",base_slot,", stop_slot - 256 = ",stop_slot - 256)
      
      ret = None
     
      while ( base_slot > (stop_slot - 256) ):
         a = idx_of_most_left_set_bit(slot_word)
         if a != None:
            target_slot = base_slot + a
            if target_slot >= stop_slot:
               ret = target_slot * point_delta
            break
         else:
            base_slot -= 256
            slot_word = 0
            if( (base_slot >> 8) in self.data.keys()) :
               slot_word = self.data[base_slot >> 8]
      return ret

   # return start point of a valued (with liquidity or order) slot that beside the given point from right,
   # NOT including the slot that embrace given point
   # return None if no valued slot found at the left of stop_slot (including stop_slot)
   # pub fn get_nearest_right_valued_slot(self, point: i32, point_delta: i32, stop_slot: i32 ) -> Option<i32>
   def get_nearest_right_valued_slot(self, point: int, point_delta: int, stop_slot: int):
      slot = int(point / point_delta)
      if point < 0 and point % point_delta != 0 :
         slot -= 1
         # round towards negative infinity
      slot += 1  # skip to the right next slot

      word_idx = slot >> 8
      bit_idx = slot % 256
      #print("slot =",slot, ", word_idx =",word_idx,", bit_idx =",bit_idx)

      slot_word = 0
      
      if( word_idx in self.data.keys()):
         # from 0001000 -> 0000111 to 1111000, then bitand to only remain equal&higher bits
         slot_word = self.data[word_idx] & ( ~((1<<bit_idx) - 1)) 
      base_slot = slot - bit_idx
      #print("slot_word = ",slot_word, ", base_slot =", base_slot, ", stop_slot =", stop_slot)
      
      ret = None

      while ( base_slot <= stop_slot ):
         a = idx_of_most_right_set_bit(slot_word)
         if a != None:
            target_slot = base_slot + a
            if target_slot <= stop_slot:
               ret = target_slot * point_delta
            break
         else:
            base_slot += 256
            slot_word = 0
            if( (base_slot >> 8) in self.data.keys()):
               slot_word = self.data[base_slot >> 8]
            #print("base_slot =",base_slot,", (base_slot >> 8) =",(base_slot >> 8),", slot_word =",slot_word)
      return ret

class UserOrder:
   def __init__(self):
      self.client_id = ""
      self.order_id = ""
      self.owner_id = ""
      self.pool_id = ""
      self.point = 0
      self.sell_token = ""
      self.buy_token = ""
      # amount through ft_transfer_call
      self.original_deposit_amount = 0
      # earn token amount through swap before actual place order
      self.swap_earn_amount = 0
      # actual original amount of this order
      self.original_amount = 0
      # total cancelled amount of this order
      self.cancel_amount = 0
      self.created_at = 0
      self.last_acc_earn = 0 # lastAccEarn
      self.remain_amount = 0 # 0 means history order, sellingRemain
      self.bought_amount = 0 # accumalated amount into inner account, earn + legacyEarn
      self.unclaimed_amount = 0 # claim will push it to inner account,

   def is_earn_y(self):
      # token1+"|"+token2+"|"+f"Fee{fee}"+"|"+str(lpt_id)
      if self.pool_id =="":
         print("Error:  pool_id is empty")
         return None

      loc = self.pool_id.find(POOL_ID_BREAK)
      if -1 == loc:
         print("Error:  E400_INVALID_POOL_ID")
         return None
      else:
         token_x = self.pool_id[0:loc]
         return token_x == self.sell_token

   def dump(self):
      print("----------------dump UserOrder-------------------")
      print("order_id:",self.order_id)
      print("owner_id:",self.owner_id)
      print("pool_id:",self.pool_id)
      print("point:",self.point)
      print("sell_token:",self.sell_token)
      print("buy_token:",self.buy_token)
      print("original_deposit_amount:",self.original_deposit_amount)
      print("swap_earn_amount:",self.swap_earn_amount)
      print("original_amount:",self.original_amount)
      print("cancel_amount:",self.cancel_amount)
      print("created_at:",self.created_at)
      print("last_acc_earn:",self.last_acc_earn)
      print("remain_amount:",self.remain_amount)
      print("bought_amount:",self.bought_amount)
      print("unclaimed_amount:",self.unclaimed_amount)
      print("----------------dump UserOrder completed-------------------")

class Pool:
   def __init__(self, parent_name = ""):
      self.pool_id = ""
      self.token_x = ""
      self.token_y = ""
      self.token_x_decimal = 1
      self.token_y_decimal = 1
      self.current_point = 0.5
      self.fee = 0
      self.point_delta = 0
      self.sqrt_price_96 = 0
      self.liquidity = 0
      self.liquidity_x = 0
      self.max_liquidity_per_point = 0
      self.fee_scale_x_128 = 0
      self.fee_scale_y_128 = 0
      self.total_fee_x_charged = 0
      self.total_fee_y_charged = 0
      self.volume_x_in = 0
      self.volume_y_in = 0
      self.volume_x_out = 0
      self.volume_y_out = 0
      self.total_liquidity = 0
      self.total_order_x = 0
      self.total_order_y = 0
      self.total_x = 0
      self.total_y = 0

      self.point_info = PointInfo()
      self.slot_bitmap = Slot_BitMap()
      self.state = RUNNING
      self.parent_name = parent_name
  
   def __str__(self):
      return "".join("pool_id:").join(str(self.pool_id))
      '''
      return "".join("pool_id:").join(str(self.pool_id))\
        .join("token_x:").join(str(self.token_x))\
        .join("token_y:").join(str(self.token_y))\
        .join("token_x_decimal:").join(str(self.token_x_decimal))\
        .join("token_y_decimal:").join(str(self.token_y_decimal))\
        .join("current_point:").join(str(self.current_point))\
        .join("fee:").join(str(self.fee))\
        .join("point_delta:").join(str(self.point_delta)) \
        .join("sqrt_price_96:").join(str(self.sqrt_price_96))\
        .join("liquidity:").join(str(self.liquidity))\
        .join("liquidity_x:").join(str(self.liquidity_x))\
        .join("max_liquidity_per_point:").join(str(self.max_liquidity_per_point))\
        .join("fee_scale_x_128:").join(str(self.fee_scale_x_128))\
        .join("fee_scale_y_128:").join(str(self.fee_scale_y_128))\
        .join("total_fee_x_charged:").join(str(self.total_fee_x_charged))\
        .join("total_fee_y_charged:").join(str(self.total_fee_y_charged))\
        .join("volume_x_in:").join(str(self.volume_x_in))\
        .join("volume_y_out:").join(str(self.volume_y_out)) \
        .join("total_liquidity:").join(str(self.total_liquidity))\
        .join("total_order_x:").join(str(self.total_order_x))\
        .join("total_order_y:").join(str(self.total_order_y))\
        .join("total_x:").join(str(self.total_x))\
        .join("total_y:").join(str(self.total_y))
      '''
   
   def dump(self):
      print("----------------dump pool: "+self.parent_name+"-------------------")
      print("pool_id:",self.pool_id)
      print("token_x:",self.token_x)
      print("token_y:",self.token_y)
      print("token_x_decimal:",self.token_x_decimal)
      print("token_y_decimal:",self.token_y_decimal)
      print("fee:",self.fee)
      print("point_delta:",self.point_delta)
      print("current_point:",self.current_point,", sqrt_price_96:",self.sqrt_price_96)
      print("liquidity:",self.liquidity)
      print("liquidity_x:",self.liquidity_x)
      print("max_liquidity_per_point:",self.max_liquidity_per_point)
      print("total_fee_x_charged:",self.total_fee_x_charged)
      print("total_fee_y_charged:",self.total_fee_y_charged)
      print("volume_x_in:",self.volume_x_in)
      print("volume_y_in:",self.volume_y_in)
      print("volume_x_out:",self.volume_x_out)
      print("volume_y_out:",self.volume_y_out)
      print("total_liquidity:",self.total_liquidity)
      print("total_order_x:",self.total_order_x)
      print("total_order_y:",self.total_order_y)
      print("total_x:",self.total_x)
      print("total_y:",self.total_y)
      print("fee_scale_x_128:",self.fee_scale_x_128)
      print("fee_scale_y_128:",self.fee_scale_y_128)
      print("----------------dump pool: "+self.parent_name+" completed-------------------")
   
   def get_liquidity(self, lpt_id):
      pass

   def list_liquidities(self, account_id, from_index, limit):
      pass

   def get_order(self, order_id):
      pass

   def find_order(self, account_id, pool_id, point):
      pass

   def list_active_orders(self, account_id):
      pass

   def list_history_orders(self, account_id):
      pass


   # Add liquidity in specified range
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @param amount_x: the number of token X users expect to add liquidity to use
   # @param amount_y: the number of token Y users expect to add liquidity to use
   # @param min_amount_x: the minimum number of token X users expect to add liquidity to use
   # @param min_amount_y: the minimum number of token Y users expect to add liquidity to use
   # @return (liquidity, need_x, need_y, acc_fee_x_in_128, acc_fee_y_in_128)
   # internal_add_liquidity(self,  left_point: i32, right_point: i32, amount_x: u128, amount_y: u128, min_amount_x: u128,  min_amount_y: u128 ) -> (u128, u128, u128, U256, U256)
   def internal_add_liquidity(self,  left_point: int, right_point: int, amount_x: int, amount_y: int, min_amount_x: int,  min_amount_y: int ):
      liquidity = self.compute_liquidity(left_point, right_point, amount_x, amount_y)
      if liquidity <= 0:
         print("liquidity <= 0 : "+self.parent_name)
         raise Exception("liquidity <= 0")         
      (acc_fee_x_in_128, acc_fee_y_in_128) = self.update_pool(left_point, right_point, liquidity)
      (need_x, need_y) = self.compute_deposit_x_y(left_point, right_point, liquidity)
      if need_x < min_amount_x or need_y < min_amount_y:
         print("need_x : "+str(need_x)+", min_amount_x : "+str(min_amount_x)+", need_y : "+str(need_y)+", min_amount_y : "+str(min_amount_y))
         print("E204_SLIPPAGE_ERR : "+self.parent_name)
         raise Exception("need_x < min_amount_x or need_y < min_amount_y")
         return (0,0,0,0,0)
      return (liquidity, need_x, need_y, acc_fee_x_in_128, acc_fee_y_in_128)


   # Removes specified number of liquidity in specified range
   # @param liquidity: the number of liquidity expected to be removed
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @param min_amount_x: removing liquidity will at least give you the number of token X
   # @param min_amount_y: removing liquidity will at least give you the number of token Y
   # @return (remove_x, remove_y, acc_fee_x_in_128, acc_fee_y_in_128)
   # internal_remove_liquidity(self, liquidity: u128, left_point: i32, right_point: i32, min_amount_x: u128, min_amount_y: u128 ) -> (u128, u128, U256, U256)
   def internal_remove_liquidity(self, liquidity: int, left_point: int, right_point: int, min_amount_x: int, min_amount_y: int ):
      #require!(liquidity <= i128::MAX as u128, E214_INVALID_LIQUIDITY)
      (acc_fee_x_in_128, acc_fee_y_in_128) = self.update_pool(left_point, right_point, -(liquidity))
      (remove_x, remove_y) = self.compute_withdraw_x_y(left_point, right_point, liquidity)
      if remove_x < min_amount_x or remove_y < min_amount_y:
         print("remove_x : "+str(remove_x)+", min_amount_x : "+str(min_amount_x)+",remove_y : "+str(remove_y)+", min_amount_y : "+str(min_amount_y))
         print("E204_SLIPPAGE_ERR : "+self.parent_name)
         raise Exception("remove_x < min_amount_x or remove_y <= min_amount_y")
         return (0,0,0,0)
      return (remove_x, remove_y, acc_fee_x_in_128, acc_fee_y_in_128)

   # Compute the token X and token Y that need to be added to add the specified liquidity in the specified range
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @param liquidity: The amount of liquidity expected to be added
   # @return (amount_x, amount_y)
   # compute_deposit_x_y(self, left_point: i32, right_point: i32, liquidity: u128 ) -> (u128, u128)
   def compute_deposit_x_y(self, left_point: int, right_point: int, liquidity: int ):
      sqrt_price_r_96 = get_sqrt_price(right_point)
      amount_y = 0
      if left_point < self.current_point:
         sqrt_price_l_96 = get_sqrt_price(left_point)
         if right_point < self.current_point:
            amount_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), True)
         else:
            amount_y = get_amount_y(liquidity, sqrt_price_l_96, self.sqrt_price_96, sqrt_rate_96(), True)
      
      amount_x = 0
      if right_point > self.current_point:
         xr_left = self.current_point + 1
         if left_point > self.current_point:
            xr_left = left_point
         amount_x = get_amount_x(liquidity, xr_left, right_point, sqrt_price_r_96, sqrt_rate_96(), True)

      if left_point <= self.current_point and right_point > self.current_point:
         amount_y += mul_fraction_ceil(liquidity, self.sqrt_price_96, pow_96())
         self.liquidity += liquidity

      return (amount_x, amount_y)


   # Compute the token X and token Y obtained by removing the specified liquidity in the specified range
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @param liquidity: The amount of liquidity expected to be removed
   # @return (amount_x, amount_y)
   # compute_withdraw_x_y(self, left_point: i32, right_point: i32, liquidity: u128 ) -> (u128, u128)
   def compute_withdraw_x_y(self, left_point: int, right_point: int, liquidity: int ):
      sqrt_price_r_96 = get_sqrt_price(right_point)
      amount_y = 0
      if left_point < self.current_point:
         sqrt_price_l_96 = get_sqrt_price(left_point)
         if right_point < self.current_point:
            amount_y = get_amount_y(liquidity, sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96(), False)
         else:
            amount_y = get_amount_y(liquidity, sqrt_price_l_96, self.sqrt_price_96, sqrt_rate_96(), False)

      amount_x = 0
      if right_point > self.current_point:
         xr_left = self.current_point + 1
         if left_point > self.current_point:
            xr_left = left_point
         amount_x = get_amount_x(liquidity, xr_left, right_point, sqrt_price_r_96, sqrt_rate_96(), False)
      
      if left_point <= self.current_point and right_point > self.current_point:
         origin_liquidity_y = self.liquidity - self.liquidity_x
         withdrawed_liquidity_y = liquidity
         if origin_liquidity_y < liquidity:
            withdrawed_liquidity_y = origin_liquidity_y

         withdrawed_liquidity_x = liquidity - withdrawed_liquidity_y
         amount_y += mul_fraction_floor(withdrawed_liquidity_y, self.sqrt_price_96, pow_96())
         amount_x += mul_fraction_floor(withdrawed_liquidity_x, pow_96(), self.sqrt_price_96)

         self.liquidity -= liquidity
         self.liquidity_x -= withdrawed_liquidity_x

      return (amount_x, amount_y)

   # The two boundary points of liquidity are updated according to the amount of liquidity change, and return the fee within range 
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @param liquidity_delta: The amount of liquidity change, it could be negative
   # @return (acc_fee_x_in_128, acc_fee_y_in_128)
   # update_pool(self,  left_point: i32, right_point: i32, liquidity_delta: i128 ) -> (U256, U256)
   def update_pool(self,  left_point: int, right_point: int, liquidity_delta: int ):
      (left_new_or_erase, right_new_or_erase) = (False, False)
      if liquidity_delta != 0:
         #print("update_pool: liquidity_delta = ",liquidity_delta, ", self.max_liquidity_per_point = ",self.max_liquidity_per_point)
         (left_new_or_erase, right_new_or_erase) = (self.point_info.update_endpoint(left_point, True, self.current_point, liquidity_delta, self.max_liquidity_per_point, self.fee_scale_x_128, self.fee_scale_y_128),
         self.point_info.update_endpoint(right_point, False, self.current_point, liquidity_delta, self.max_liquidity_per_point, self.fee_scale_x_128, self.fee_scale_y_128))

      (acc_fee_x_in_128, acc_fee_y_in_128) = self.point_info.get_fee_in_range(left_point, right_point, self.current_point, self.fee_scale_x_128, self.fee_scale_y_128)

      if left_new_or_erase: 
         left_endpoint = self.point_info.get_point_data(left_point)
         if left_endpoint is None:
            print("current_point =",self.current_point)
         if left_endpoint.has_liquidity(): # new endpoint for liquidity
            #self.slot_bitmap.dump()
            self.slot_bitmap.set_one(left_point, self.point_delta)
            #self.slot_bitmap.dump()
         else: # removed endpoint for liquidity
            left_endpoint.liquidity_data = None
            if False == left_endpoint.has_active_order():
               #self.slot_bitmap.dump()
               self.slot_bitmap.set_zero(left_point, self.point_delta)
               #self.slot_bitmap.dump()
               
            if left_endpoint.has_order():
              self.point_info.set_point_data(left_point, left_endpoint)
            else:
              self.point_info.remove(left_point)  
         
             

      if right_new_or_erase:
         right_endpoint = self.point_info.get_point_data(right_point)
         if right_endpoint is None:
            print("current_point =",self.current_point)
         if right_endpoint.has_liquidity(): # new endpoint for liquidity
            #self.slot_bitmap.dump()
            self.slot_bitmap.set_one(right_point, self.point_delta)
            #self.slot_bitmap.dump()
         else: # removed endpoint for liquidity
            right_endpoint.liquidity_data = None;
            if False == right_endpoint.has_active_order():
               #self.slot_bitmap.dump()
               self.slot_bitmap.set_zero(right_point, self.point_delta)
               #self.slot_bitmap.dump()
            if right_endpoint.has_order():
               self.point_info.set_point_data(right_point, right_endpoint)
            else:
               self.point_info.remove(right_point) 

      return (acc_fee_x_in_128, acc_fee_y_in_128)


   # compute how much liquidity the specified token X and token Y can add in the specified range
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @param amount_x: the number of token X users expect to add liquidity to use
   # @param amount_y: the number of token Y users expect to add liquidity to use
   # @return liquidity
   # compute_liquidity(self,  left_point: i32, right_point: i32, amount_x: u128, amount_y: u128) -> u128
   def compute_liquidity(self,  left_point: int, right_point: int, amount_x: int, amount_y: int):
      liquidity = ( (1<<128) - 1 ) // 2
      (x, y) = self.compute_deposit_xy_per_unit(left_point, right_point)
      if x > 0:
         xl = mul_fraction_floor(amount_x, pow_96(), x)
         if liquidity > xl:
            liquidity = xl

      if y > 0:
         yl = mul_fraction_floor(amount_y - 1, pow_96(), y)
         if liquidity > yl:
            liquidity = yl

      return liquidity

   # compute the amount of token X and token Y required to add a unit of liquidity within a specified range
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @return (x, y)
   # compute_deposit_xy_per_unit(self,  left_point: i32, right_point: i32) -> (U256, U256)
   def compute_deposit_xy_per_unit(self,  left_point: int, right_point: int):
      sqrt_price_r_96 = get_sqrt_price(right_point)
      y = 0
      if left_point < self.current_point:
         sqrt_price_l_96 = get_sqrt_price(left_point)
         if right_point < self.current_point:
            y = get_amount_y_unit_liquidity_96(sqrt_price_l_96, sqrt_price_r_96, sqrt_rate_96())
         else:
            y = get_amount_y_unit_liquidity_96(sqrt_price_l_96, self.sqrt_price_96, sqrt_rate_96())
         
      x = 0
      if right_point > self.current_point:
         xr_left = self.current_point + 1
         if left_point > self.current_point:
            xr_left = left_point
         x = get_amount_x_unit_liquidity_96(xr_left, right_point, sqrt_price_r_96, sqrt_rate_96())

      if left_point <= self.current_point and right_point > self.current_point:
         y += self.sqrt_price_96
      
      return (x, y)






   ####################################################################################################
   # Process limit_order_y at current point
   # @param protocol_fee_rate
   # @param order_data
   # @param amount_x
   # @return (is_finished, consumed_x, gained_y)
   # fn process_limit_order_y(&mut self, pool_fee: u32, protocol_fee_rate: u32, order_data: &mut OrderData, amount_x: u128) -> (bool, u128, u128, u128, u128)
   def process_limit_order_y(self, pool_fee: int, protocol_fee_rate: int, order_data: OrderData, amount_x: int):
      is_finished = False
      net_amount = mul_fraction_floor(amount_x, (10**6 - pool_fee), 10**6)
      if net_amount > 0:
         (cost_x, acquire_y) = x_swap_y_at_price(net_amount, self.sqrt_price_96, order_data.selling_y)
         if acquire_y < order_data.selling_y or cost_x >= net_amount:
            is_finished = True

         fee_amount = 0
         if cost_x >= net_amount:
            # all x consumed
            fee_amount = amount_x - cost_x
         else:
            fee_amount = mul_fraction_ceil(cost_x, pool_fee, (10**6 - pool_fee))
         
         # limit order fee goes to lp and protocol
         protocol_fee = 0
         if self.liquidity != 0:
            charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
            self.total_fee_x_charged += charged_fee_amount
            self.fee_scale_x_128 += mul_fraction_floor((fee_amount - charged_fee_amount), pow_128(), self.liquidity)
            protocol_fee = charged_fee_amount
         else:
            self.total_fee_x_charged += fee_amount
            protocol_fee = fee_amount


         # for statistic
         self.total_order_y -= acquire_y
         
         order_data.selling_y -= acquire_y
         order_data.earn_x += cost_x
         order_data.acc_earn_x += cost_x

         if order_data.selling_y == 0:
            # point order fulfilled, handle legacy logic
            order_data.earn_x_legacy += order_data.earn_x
            order_data.acc_earn_x_legacy = order_data.acc_earn_x
            order_data.earn_x = 0
         return (is_finished, cost_x + fee_amount, acquire_y, fee_amount, protocol_fee)
      else:
         return (True, 0, 0, 0, 0)

   # Process liquidity_x in range [left_pt, self.current_point)
   # @param protocol_fee_rate
   # @param amount_x
   # @param left_pt
   # @return (is_finished, consumed_x, gained_y)
   # fn process_liquidity_y(&mut self, pool_fee: u32, protocol_fee_rate: u32, amount_x: u128, left_pt: i32) -> (bool, u128, u128, u128, u128)
   def process_liquidity_y(self, pool_fee: int, protocol_fee_rate: int, amount_x: int, left_pt: int):
      net_amount = mul_fraction_floor(amount_x, (10**6 - pool_fee), 10**6)
      if net_amount > 0:
         if self.liquidity > 0:
            x2y_range_result = self.range_x_swap_y(left_pt, net_amount, pool_fee, protocol_fee_rate)
            #print("left_pt = ",left_pt,", x2y_range_result.final_pt =",x2y_range_result.final_pt)
            
            fee_amount = amount_x - x2y_range_result.cost_x
            if x2y_range_result.cost_x < net_amount:
               fee_amount = mul_fraction_ceil(x2y_range_result.cost_x, pool_fee, (10**6 - pool_fee))

            # distribute fee
            charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
            self.total_fee_x_charged += charged_fee_amount
            self.fee_scale_x_128 += mul_fraction_floor((fee_amount - charged_fee_amount),pow_128(), self.liquidity)               
              
            # update current point liquidity info
            self.current_point = x2y_range_result.final_pt
            self.sqrt_price_96 = x2y_range_result.sqrt_final_price_96
            self.liquidity_x = x2y_range_result.liquidity_x

            return (x2y_range_result.finished, x2y_range_result.cost_x + fee_amount, x2y_range_result.acquire_y, fee_amount, charged_fee_amount)
         else:
            # swap hasn't completed but current range has no liquidity_y 
            if self.current_point != left_pt:
               self.current_point = left_pt
               self.sqrt_price_96 = get_sqrt_price(left_pt)
               self.liquidity_x = 0

            return (False, 0, 0, 0, 0)
      else:
         # swap has already completed
         return (True, 0, 0, 0, 0)

   # Process x_swap_y with amount of token X, which is swapping to the left 
   # @param protocol_fee_rate
   # @param input_amount: amount of token X
   # @param low_boundary_point: swap won't pass this point
   # @param is_quote: whether is it called by a quote interface
   # @return (consumed_x, gained_y, is_finished)
   # internal_x_swap_y(&mut self, pool_fee: u32, protocol_fee_rate: u32, input_amount: u128, low_boundary_point: i32, is_quote: bool) -> (u128, u128, bool, u128, u128)
   def internal_x_swap_y(self, pool_fee: int, protocol_fee_rate: int, input_amount: int, low_boundary_point: int, is_quote: bool):
      boundary_point = max(low_boundary_point, LEFT_MOST_POINT)
      amount = input_amount
      amount_x = 0
      amount_y = 0
      is_finished = False
      total_fee = 0
      protocol_fee = 0
      #print("internal_x_swap_y, current_point =",self.current_point)
      while (boundary_point <= self.current_point and is_finished == False):
         current_order_or_endpt = self.point_info.get_point_type_value(self.current_point, self.point_delta)
         # step1: process possible limit order on current point
         #print("current_order_or_endpt = ",current_order_or_endpt)
         if (current_order_or_endpt & 2) > 0:
            # process limit order
            point_data = None
            order_data = None
            if self.current_point in self.point_info.data.keys():
               point_data = self.point_info.data[self.current_point]
               order_data = point_data.order_data
            
            #self.point_info.dump()
            (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_limit_order_y(pool_fee, protocol_fee_rate, order_data, amount)
            is_finished = process_ret0
            (amount, amount_x, amount_y, total_fee, protocol_fee) = (amount-process_ret1, amount_x+process_ret1, amount_y+process_ret2, total_fee + process_ret3, protocol_fee + process_ret4 )

            # stats for limit order
            stats_data = self.point_info.get_point_stats_data_or_default(self.current_point)
            stats_data.order_volume_y_out += process_ret2
            stats_data.order_volume_x_in += process_ret1 + process_ret3
            
            # fee
            stats_data.p_fee_x += process_ret3 * protocol_fee_rate // BP_DENOM
            stats_data.fee_x += process_ret3 - stats_data.p_fee_x
            
            self.point_info.set_point_stats_data(self.current_point, stats_data)   


            self.update_point_order( point_data, order_data, is_quote)
            #order_data.dump()

            if is_finished:
               break
         
         # step 2: process possible liquidity on current point
         
         # the current_point unmoved cause it may be in the middle of some liquidity slot, need to be processed in next step
         search_start = self.current_point - 1

         if ( current_order_or_endpt & 1 ) > 0:
            # current point is an liquidity endpoint, process liquidity
            (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_liquidity_y(pool_fee, protocol_fee_rate, amount, self.current_point)
            is_finished = process_ret0
            (amount, amount_x, amount_y, total_fee, protocol_fee) = (amount-process_ret1, amount_x+process_ret1, amount_y+process_ret2, total_fee + process_ret3, protocol_fee + process_ret4 )

            if is_finished == False:
               # pass endpoint
               self.pass_endpoint(self.current_point, is_quote, True)
               # move one step to the left
               self.current_point -= 1
               self.sqrt_price_96 = get_sqrt_price(self.current_point)
               self.liquidity_x = 0

            if (is_finished or self.current_point < boundary_point):
               break
               
            # new current point is an endpoint or has order, only exist in point_delta==1
            if self.point_info.get_point_type_value(self.current_point, self.point_delta) & 3 > 0:
               continue
            
            search_start = self.current_point

         # step 3a: locate the left point for a range swapping headig to the left
         lack_one_point_to_real_left = False
         next_pt = boundary_point
         #print("endpoints =",self.slot_bitmap.get_endpoints(40))
         point = self.slot_bitmap.get_nearest_left_valued_slot(search_start, self.point_delta, boundary_point / self.point_delta)
         #print("nearest_left_valued_point = ",point)
         if point != None:
            if point < boundary_point:
               next_pt = boundary_point
            else:
               if self.point_info.get_point_type_value(point, self.point_delta) & 2 > 0:
                  lack_one_point_to_real_left = True
                  # case 1: current_point is middle point and found left point is adjacent to it, then we actually need to do a single current point swap using process_liquidity_y;
                  # case 2: otherwise, we increase left point to protect order on left point;
                  next_pt = point + 1
               else:
                  next_pt = point
         
         # step 3b: do range swap according to the left point located in step 3a
         (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_liquidity_y(pool_fee, protocol_fee_rate, amount, next_pt)
         is_finished = process_ret0
         (amount, amount_x, amount_y, total_fee, protocol_fee) = (amount-process_ret1, amount_x+process_ret1, amount_y+process_ret2, total_fee + process_ret3, protocol_fee + process_ret4)
         
         # check the swap is completed or not
         if is_finished or self.current_point <= boundary_point:
            break

         # Now, is_finished == false && self.current_point > boundary_point
         # adjust current point if necessary 
         if lack_one_point_to_real_left:
            # must move 1 left, otherwise infinite loop
            self.current_point -= 1
            self.sqrt_price_96 = get_sqrt_price(self.current_point)
            self.liquidity_x = 0

      return (amount_x, amount_y, is_finished, total_fee, protocol_fee)


   # Process limit_order_x at current point
   # @param protocol_fee_rate
   # @param order_data
   # @param amount_y
   # @return (is_finished, consumed_y, gained_x)
   # process_limit_order_x(&mut self, pool_fee: u32, protocol_fee_rate: u32, order_data: &mut OrderData, amount_y: u128) -> (bool, u128, u128, u128, u128) 
   def process_limit_order_x(self, pool_fee: int, protocol_fee_rate: int, order_data: OrderData, amount_y: int):
      is_finished = False
      net_amount = mul_fraction_floor(amount_y, (10**6 - pool_fee), 10**6)
      if net_amount > 0:
         (cost_y, acquire_x) = y_swap_x_at_price( net_amount, self.sqrt_price_96, order_data.selling_x )
         if acquire_x < order_data.selling_x or cost_y >= net_amount:
            is_finished = True

         fee_amount = amount_y - cost_y
         if cost_y < net_amount:
            fee_amount = mul_fraction_ceil(cost_y, pool_fee, (10**6 - pool_fee))

         # limit order fee goes to lp and protocol
         protocol_fee = 0
         if self.liquidity != 0:
            charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
            self.total_fee_y_charged += charged_fee_amount
            self.fee_scale_y_128 += mul_fraction_floor((fee_amount - charged_fee_amount), pow_128(), self.liquidity)
            protocol_fee = charged_fee_amount
         else:
            self.total_fee_y_charged += fee_amount
            protocol_fee = fee_amount

         # for statistic
         self.total_order_x -= acquire_x
         
         order_data.selling_x -= acquire_x
         order_data.earn_y += cost_y
         order_data.acc_earn_y += cost_y

         if order_data.selling_x == 0:
            # point order fulfilled, handle legacy logic
            order_data.earn_y_legacy += order_data.earn_y
            order_data.acc_earn_y_legacy = order_data.acc_earn_y
            order_data.earn_y = 0

         return (is_finished, cost_y + fee_amount, acquire_x, fee_amount, protocol_fee)
      else:
         return (True, 0, 0, 0, 0)


   # Process liquidity_x in range
   # @param protocol_fee_rate
   # @param amount_y
   # @param next_pt
   # @return (is_finished, consumed_y, gained_x)
   # process_liquidity_x(&mut self, pool_fee: u32, protocol_fee_rate: u32, amount_y: u128, next_pt: i32) -> (bool, u128, u128, u128, u128)
   def process_liquidity_x(self, pool_fee: int, protocol_fee_rate: int, amount_y: int, next_pt: int):
      net_amount = mul_fraction_floor(amount_y, (10**6 - pool_fee), 10**6)
      if net_amount > 0:
         y2x_range_result =  self.range_y_swap_x(next_pt, net_amount, pool_fee, protocol_fee_rate)
         
         fee_amount = amount_y - y2x_range_result.cost_y
         if y2x_range_result.cost_y < net_amount:
            fee_amount = mul_fraction_ceil(y2x_range_result.cost_y, pool_fee, (10**6 - pool_fee))
         
         charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
         self.total_fee_y_charged += charged_fee_amount
         self.fee_scale_y_128 += mul_fraction_floor((fee_amount - charged_fee_amount), pow_128(), self.liquidity)

         self.current_point = y2x_range_result.final_pt
         self.sqrt_price_96 = y2x_range_result.sqrt_final_price_96
         self.liquidity_x = y2x_range_result.liquidity_x
         return (y2x_range_result.finished, y2x_range_result.cost_y + fee_amount, y2x_range_result.acquire_x, fee_amount, charged_fee_amount)
      else:
         return (True, 0, 0, 0, 0)


   # Process y_swap_x in range
   # @param protocol_fee_rate
   # @param input_amount: amount of token Y
   # @param hight_boundary_point
   # @param is_quote: whether the quote function is calling
   # @return (consumed_y, gained_x, is_finished)
   # internal_y_swap_x(&mut self, pool_fee: u32, protocol_fee_rate: u32, input_amount: u128, hight_boundary_point: i32, is_quote: bool) -> (u128, u128, bool, u128, u128)
   def internal_y_swap_x(self, pool_fee: int, protocol_fee_rate: int, input_amount: int, hight_boundary_point: int, is_quote: bool):
      boundary_point = min(hight_boundary_point, RIGHT_MOST_POINT)
      amount = input_amount
      amount_x = 0
      amount_y = 0
      is_finished = False
      total_fee = 0
      protocol_fee = 0
      current_order_or_endpt  = self.point_info.get_point_type_value(self.current_point, self.point_delta)
      #print("internal_y_swap_x, current_point =",self.current_point)
      while self.current_point < boundary_point and is_finished == False:
         if (current_order_or_endpt & 2) > 0:
            # process limit order
            point_data = None
            order_data = None
            if self.current_point in self.point_info.data.keys():
               point_data = self.point_info.data[self.current_point]
               order_data = point_data.order_data
            
            (process_ret0,process_ret1,process_ret2,process_ret3,process_ret4) = self.process_limit_order_x(pool_fee, protocol_fee_rate, order_data, amount)
            is_finished = process_ret0
            (amount, amount_x, amount_y, total_fee, protocol_fee) = (amount-process_ret1, amount_x+process_ret2, amount_y+process_ret1, total_fee+process_ret3, protocol_fee+process_ret4)
            
            # stats for limit order
            stats_data = self.point_info.get_point_stats_data_or_default(self.current_point)
            stats_data.order_volume_y_in += process_ret1 + process_ret3
            stats_data.order_volume_x_out += process_ret2
            
            # fee
            stats_data.p_fee_y += process_ret3 * protocol_fee_rate // BP_DENOM
            stats_data.fee_y += process_ret3 - stats_data.p_fee_y
            
            self.point_info.set_point_stats_data(self.current_point, stats_data)
            # end of stats for limit order
            
            self.update_point_order(point_data, order_data, is_quote)

            if is_finished:
               break
         
         (next_pt, next_val)= (boundary_point, 0)
         point = self.slot_bitmap.get_nearest_right_valued_slot(self.current_point, self.point_delta, boundary_point / self.point_delta)
         #print("nearest_right_valued_point = ",point)
         if point != None:
            if point > boundary_point:
               (next_pt, next_val) = (boundary_point, 0)
            else:
               (next_pt, next_val) = (point, self.point_info.get_point_type_value(point, self.point_delta))
         
         if self.liquidity == 0:
            # no liquidity in the range [self.current_point, next_pt)
            self.current_point = next_pt
            self.sqrt_price_96 = get_sqrt_price(self.current_point)
            if next_val & 1 > 0:
               # pass endpoint
               self.pass_endpoint(next_pt, is_quote, False)
               self.liquidity_x = self.liquidity
            current_order_or_endpt = next_val
         else:
            # process range liquidity [self.current_point, next_pt)
            (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_liquidity_x(pool_fee, protocol_fee_rate, amount, next_pt)
            is_finished = process_ret0

            if self.current_point == next_pt:
               if (next_val & 1) > 0:
                  # pass endpoint
                  self.pass_endpoint(next_pt, is_quote, False)
               self.liquidity_x = self.liquidity
               current_order_or_endpt = next_val
            else:
               current_order_or_endpt = 0

            (amount, amount_x, amount_y, total_fee, protocol_fee) = (amount-process_ret1, amount_x+process_ret2, amount_y+process_ret1, total_fee + process_ret3, protocol_fee + process_ret4)

      return (amount_y, amount_x, is_finished, total_fee, protocol_fee)

   # Process limit_order_y by desire_y at current point
   # @param protocol_fee_rate
   # @param order_data
   # @param desire_y
   # @return (is_finished, consumed_x, gained_y)
   # process_limit_order_y_desire_y(&mut self, pool_fee: u32, protocol_fee_rate: u32, order_data: &mut OrderData, desire_y: u128) -> (bool, u128, u128, u128, u128)
   def process_limit_order_y_desire_y(self, pool_fee: int, protocol_fee_rate: int, order_data: OrderData, desire_y: int):
      is_finished = False
      (cost_x, acquire_y) = x_swap_y_at_price_desire( desire_y, self.sqrt_price_96, order_data.selling_y)
      if acquire_y >= desire_y:
         is_finished = True

      # limit order fee goes to lp and protocol
      fee_amount = mul_fraction_ceil(cost_x, pool_fee, (10**6 - pool_fee))
      protocol_fee = 0
      if self.liquidity != 0:
         charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
         self.total_fee_x_charged += charged_fee_amount
         self.fee_scale_x_128 += mul_fraction_floor((fee_amount - charged_fee_amount), pow_128(), self.liquidity)
         protocol_fee = charged_fee_amount
      else:
         self.total_fee_x_charged += fee_amount
         protocol_fee = fee_amount

      # for statistic
      self.total_order_y -= acquire_y
      
      order_data.selling_y -= acquire_y
      order_data.earn_x += cost_x
      order_data.acc_earn_x += cost_x

      if order_data.selling_y == 0:
         # point order fulfilled, handle legacy logic
         order_data.earn_x_legacy += order_data.earn_x
         order_data.earn_x = 0
         order_data.acc_earn_x_legacy = order_data.acc_earn_x

      return (is_finished, cost_x + fee_amount, acquire_y, fee_amount, protocol_fee)


   # Process liquidity_y by desire_y in range
   # @param protocol_fee_rate
   # @param desire_y
   # @param left_pt
   # @return (is_finished, consumed_x, gained_y)
   # process_liquidity_y_desire_y(&mut self, pool_fee: u32, protocol_fee_rate: u32, desire_y: u128, left_pt: i32) -> (bool, u128, u128, u128, u128)
   def process_liquidity_y_desire_y(self, pool_fee: int, protocol_fee_rate: int, desire_y: int, left_pt: int):
      if desire_y > 0:
         if self.liquidity > 0:
            x2y_range_desire_result = self.range_x_swap_y_desire(left_pt, desire_y, pool_fee, protocol_fee_rate)

            fee_amount = mul_fraction_ceil(x2y_range_desire_result.cost_x, pool_fee, (10**6 - pool_fee))
            # distribute fee
            charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
            self.total_fee_x_charged += charged_fee_amount
            self.fee_scale_x_128 += mul_fraction_floor((fee_amount - charged_fee_amount), pow_128(), self.liquidity)
            
            # update current point liquidity info
            self.current_point = x2y_range_desire_result.final_pt
            self.sqrt_price_96 = x2y_range_desire_result.sqrt_final_price_96
            self.liquidity_x = x2y_range_desire_result.liquidity_x
            return (x2y_range_desire_result.finished, (x2y_range_desire_result.cost_x + fee_amount), x2y_range_desire_result.acquire_y)
         else:
            # swap hasn't completed but current range has no liquidity_y
            if self.current_point != left_pt:
               self.current_point = left_pt
               self.sqrt_price_96 = get_sqrt_price(left_pt)
               self.liquidity_x = 0
            return (False, 0, 0, 0, 0)
      else:
         # swap has already completed
         (True, 0, 0, 0, 0)

   # Process x_swap_y by desire_y in range
   # @param protocol_fee_rate
   # @param desire_y
   # @param low_boundary_point
   # @param is_quote: whether the quote function is calling
   # @return (consumed_x, gained_y, is_finished)
   # internal_x_swap_y_desire_y(&mut self, pool_fee: u32, protocol_fee_rate: u32, desire_y: u128, low_boundary_point: i32, is_quote: bool) -> (u128, u128, bool, u128, u128)
   def internal_x_swap_y_desire_y(self, pool_fee: int, protocol_fee_rate: int, desire_y: int, low_boundary_point: int, is_quote: bool):
      if(desire_y <= 0):
         print("E205_INVALID_DESIRE_AMOUNT")
         return (0,0,False)
      
      boundary_point = max(low_boundary_point, LEFT_MOST_POINT)
      is_finished = False
      amount_x = 0
      amount_y = 0
      desire_y = desire_y
      total_fee = 0
      protocol_fee = 0
      
      
      while boundary_point <= self.current_point and is_finished == False:
         current_order_or_endpt  = self.point_info.get_point_type_value(self.current_point, self.point_delta)
         # step1: process possible limit order on current point
         if (current_order_or_endpt & 2) > 0:
            # process limit order
            point_data = None
            order_data = None
            if self.current_point in self.point_info.data.keys():
               point_data = self.point_info.data[self.current_point]
               order_data = point_data.order_data
            
            (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_limit_order_y_desire_y(pool_fee, protocol_fee_rate, order_data, desire_y)
            is_finished = process_ret0
            desire_y = 0
            if desire_y > process_ret2:
               desire_y = desire_y - process_ret2
            
            (amount_x, amount_y, total_fee, protocol_fee) = (amount_x + process_ret1, amount_y + process_ret2, total_fee+process_ret3, protocol_fee+process_ret4)

            # stats for limit order
            stats_data = self.point_info.get_point_stats_data_or_default(self.current_point)
            stats_data.order_volume_y_out += process_ret2
            stats_data.order_volume_x_in += process_ret1 + process_ret3
            
            # fee
            stats_data.p_fee_x += process_ret3 * protocol_fee_rate // BP_DENOM
            stats_data.fee_x += process_ret3 - stats_data.p_fee_x
            
            self.point_info.set_point_stats_data(self.current_point, stats_data)
            # end of stats for limit order

            self.update_point_order(point_data, order_data, is_quote)

            if is_finished:
               break

         # step 2: process possible liquidity on current point
         search_start = self.current_point - 1

         if (current_order_or_endpt & 1) > 0:
            (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_liquidity_y_desire_y(pool_fee, protocol_fee_rate, desire_y, self.current_point)
            is_finished = process_ret0
            (desire_y, amount_x, amount_y, total_fee, protocol_fee) = (desire_y - min(desire_y, process_ret2), amount_x+process_ret1, amount_y+process_ret2, total_fee+process_ret3, protocol_fee+process_ret4)
            
            if is_finished == False:
               # pass endpoint
               self.pass_endpoint(self.current_point, is_quote, True)
               # move one step to the left
               self.current_point -= 1
               self.sqrt_price_96 = get_sqrt_price(self.current_point)
               self.liquidity_x = 0

            if is_finished or self.current_point < boundary_point:
               break
               
            # new current point is an endpoint or has order, only exist in point_delta==1
            if self.point_info.get_point_type_value(self.current_point, self.point_delta) & 3 > 0:
               continue
            
            search_start = self.current_point

         # step 3a: locate the left point for a range swapping headig to the left
         lack_one_point_to_real_left = False
         next_pt = boundary_point
         #print("endpoints =",self.slot_bitmap.get_endpoints(40))
         point = self.slot_bitmap.get_nearest_left_valued_slot(search_start, self.point_delta, boundary_point / self.point_delta)
         #print("nearest_left_valued_point = ",point)
         if point != None:
            if point < boundary_point:
               next_pt = boundary_point
            else:
               if self.point_info.get_point_type_value(point, self.point_delta) & 2 > 0:
                  lack_one_point_to_real_left = True
                  # case 1: current_point is middle point and found left point is adjacent to it, then we actually need to do a single current point swap using process_liquidity_y;
                  # case 2: otherwise, we increase left point to protect order on left point;
                  next_pt = point + 1
               else:
                  next_pt = point
         
         # step 3b: do range swap according to the left point located in step 3a
         (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_liquidity_y_desire_y(pool_fee, protocol_fee_rate, amount, next_pt)
         is_finished = process_ret0
         (amount, amount_x, amount_y, total_fee, protocol_fee) = (amount-process_ret1, amount_x+process_ret1, amount_y+process_ret2, total_fee + process_ret3, protocol_fee + process_ret4)
         
         # check the swap is completed or not
         if is_finished or self.current_point <= boundary_point:
            break

         # Now, is_finished == false && self.current_point > boundary_point
         # adjust current point if necessary 
         if lack_one_point_to_real_left:
            # must move 1 left, otherwise infinite loop
            self.current_point -= 1
            self.sqrt_price_96 = get_sqrt_price(self.current_point)
            self.liquidity_x = 0

      return (amount_x, amount_y, is_finished, total_fee, protocol_fee)
   
   
   # Process limit_order_x by desire_x at current point
   # @param protocol_fee_rate
   # @param order_data
   # @param desire_x
   # @return (is_finished, consumed_y, gained_x)
   # process_limit_order_x_desire_x(&mut self, pool_fee: u32, protocol_fee_rate: u32, order_data: &mut OrderData, desire_x: u128) -> (bool, u128, u128, u128, u128)
   def process_limit_order_x_desire_x(self, pool_fee: int, protocol_fee_rate: int, order_data: OrderData, desire_x: int):
      is_finished = False
      (cost_y, acquire_x) = y_swap_x_at_price_desire( desire_x, self.sqrt_price_96, order_data.selling_x)
      if acquire_x >= desire_x:
         is_finished = True

      # limit order fee goes to lp and protocol
      fee_amount = mul_fraction_ceil(cost_y, pool_fee, (10**6 - pool_fee))
      protocol_fee = 0
      if self.liquidity != 0:
         charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
         self.total_fee_y_charged += charged_fee_amount
         self.fee_scale_y_128 += mul_fraction_floor((fee_amount - charged_fee_amount), pow_128(), self.liquidity)
         protocol_fee = charged_fee_amount
      else:
         self.total_fee_y_charged += fee_amount
         protocol_fee = fee_amount

      # for statistic
      self.total_order_x -= acquire_x

      order_data.selling_x -= acquire_x
      order_data.earn_y += cost_y
      order_data.acc_earn_y += cost_y

      if order_data.selling_x == 0:
         # point order fulfilled, handle legacy logic
         order_data.earn_y_legacy += order_data.earn_y
         order_data.earn_y = 0
         order_data.acc_earn_y_legacy = order_data.acc_earn_y
      
      return (is_finished, cost_y + fee_amount, acquire_x, fee_amount, protocol_fee)


   # Process liquidity_x by desire_x in range
   # @param protocol_fee_rate
   # @param desire_x
   # @param next_pt
   # @return (is_finished, consumed_y, gained_x)
   # process_liquidity_x_desire_x(&mut self, pool_fee: u32, protocol_fee_rate: u32, desire_x: u128, next_pt: i32) -> (bool, u128, u128, u128, u128)
   def process_liquidity_x_desire_x(self, pool_fee: int, protocol_fee_rate: int, desire_x: int, next_pt: int):
      if desire_x > 0:
         y2x_range_desire_result = self.range_y_swap_x_desire(next_pt, desire_x, pool_fee, protocol_fee_rate)

         fee_amount = mul_fraction_ceil(y2x_range_desire_result.cost_y, pool_fee, (10**6 - pool_fee))
         charged_fee_amount = fee_amount * protocol_fee_rate // BP_DENOM
         self.total_fee_y_charged += charged_fee_amount
         self.fee_scale_y_128 += mul_fraction_floor((fee_amount - charged_fee_amount), pow_128(), self.liquidity)

         self.current_point = y2x_range_desire_result.final_pt
         self.sqrt_price_96 = y2x_range_desire_result.sqrt_final_price_96
         self.liquidity_x = y2x_range_desire_result.liquidity_x
         return (y2x_range_desire_result.finished, (y2x_range_desire_result.cost_y + fee_amount), y2x_range_desire_result.acquire_x, fee_amount, charged_fee_amount)
      else:
         return (True, 0, 0, 0, 0)

   # Process y_swap_x by desire_x in range
   # @param protocol_fee_rate
   # @param desire_x
   # @param high_boundary_point
   # @param is_quote: whether the quote function is calling
   # @return (consumed_y, gained_x, is_finished)
   # internal_y_swap_x_desire_x(&mut self, pool_fee: u32, protocol_fee_rate: u32, desire_x: u128, high_boundary_point: i32, is_quote: bool) -> (u128, u128, bool, u128, u128)
   def internal_y_swap_x_desire_x(self, pool_fee: int, protocol_fee_rate: int, desire_x: int, high_boundary_point: int, is_quote: bool):
      if(desire_x <= 0):
         print("E205_INVALID_DESIRE_AMOUNT")
         return (0,0,False)
      
      boundary_point = min(high_boundary_point, RIGHT_MOST_POINT)
      is_finished = False
      amount_x = 0
      amount_y = 0
      desire_x = desire_x
      total_fee = 0
      protocol_fee = 0
      current_order_or_endpt  = self.point_info.get_point_type_value(self.current_point, self.point_delta)
      
      while self.current_point < boundary_point and is_finished == False:
         if current_order_or_endpt & 2 > 0:
            # process limit order
            point_data = None
            order_data = None
            if self.current_point in self.point_info.data.keys():
               point_data = self.point_info.data[self.current_point]
               order_data = point_data.order_data
            
            process_ret0, process_ret1, process_ret2, process_ret3, process_ret4 = self.process_limit_order_x_desire_x(pool_fee, protocol_fee_rate, order_data, desire_x)
            is_finished = process_ret0
            desire_x = 0
            if desire_x > process_ret2:
               desire_x = desire_x - process_ret2
            (amount_x, amount_y, total_fee, protocol_fee) = (amount_x + process_ret2, amount_y + process_ret1, total_fee+process_ret3, protocol_fee+process_ret4)

            # stats for limit order
            stats_data = self.point_info.get_point_stats_data_or_default(self.current_point)
            stats_data.order_volume_y_in += process_ret1 + process_ret3
            stats_data.order_volume_x_out += process_ret2
            
            # fee
            stats_data.p_fee_y += process_ret3 * protocol_fee_rate // BP_DENOM
            stats_data.fee_y += process_ret3 - stats_data.p_fee_y
            
            self.point_info.set_point_stats_data(self.current_point, stats_data)
            # end of stats for limit order

            self.update_point_order(point_data, order_data, is_quote)

            if is_finished:
               break
         
         (next_pt, next_val) = (boundary_point, 0)
         point = self.slot_bitmap.get_nearest_right_valued_slot(self.current_point, self.point_delta, boundary_point / self.point_delta)
         if point != None:
            if point > boundary_point:
               (next_pt, next_val) = (boundary_point, 0)
            else:
                (next_pt, next_val) = (point, self.point_info.get_point_type_value(point, self.point_delta))
         
         if self.liquidity == 0:
            self.current_point = next_pt
            self.sqrt_price_96 = get_sqrt_price(self.current_point)
            if (next_val & 1) > 0:
               self.pass_endpoint(next_pt, is_quote, False)
               self.liquidity_x = self.liquidity
            current_order_or_endpt = next_val
         else:
            (process_ret0, process_ret1, process_ret2, process_ret3, process_ret4) = self.process_liquidity_x_desire_x(pool_fee, protocol_fee_rate, desire_x, next_pt)
            is_finished = process_ret0
            (desire_x, amount_x, amount_y, total_fee, protocol_fee) = (desire_x - min(desire_x, process_ret2), amount_x+process_ret2, amount_y+process_ret1, total_fee+process_ret3, protocol_fee+process_ret4)

            if self.current_point == next_pt:
               if (next_val & 1) > 0:
                  self.pass_endpoint(next_pt, is_quote, False)
               self.liquidity_x = self.liquidity
               current_order_or_endpt = next_val
            else:
               current_order_or_endpt = 0

      return (amount_y, amount_x, is_finished, total_fee, protocol_fee)


   # @param left_point: the left boundary of range
   # @param amount_x: the amount of token X to swap-in
   # @return X2YRangeRet
   # range_x_swap_y(&mut self, left_point: i32, amount_x: u128) -> X2YRangeRet   
   def range_x_swap_y(self, left_point: int, amount_x: int, pool_fee: int, protocol_fee_rate: int):
      result = X2YRangeRet()
      amount_x = amount_x

      current_has_y = self.liquidity_x < self.liquidity
      if (current_has_y and (self.liquidity_x > 0 or left_point == self.current_point)):
         # current point as a special point to swap first
         (at_price_cost_x, at_price_acquire_y, at_price_liquidity_x) = x_swap_y_at_price_liquidity(amount_x, self.sqrt_price_96, self.liquidity, self.liquidity_x)
         result.cost_x = at_price_cost_x
         result.acquire_y = at_price_acquire_y
         result.liquidity_x = at_price_liquidity_x
         if (at_price_liquidity_x < self.liquidity or  at_price_cost_x >= amount_x):
            result.finished = True
            result.final_pt = self.current_point
            result.sqrt_final_price_96 = self.sqrt_price_96
         else:
            amount_x -= at_price_cost_x
         
         ############################################################################         
         # update point_stats_data
         current_endpoint = self.current_point // self.point_delta         

         stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)
         
         # fee
         endpoint_fee_amount = mul_fraction_ceil(at_price_cost_x, pool_fee, (10**6 - pool_fee))
         endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
         stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
         stats_data.p_fee_x += endpoint_charged_fee_amount
         
         stats_data.liquidity_volume_x_in += at_price_cost_x + endpoint_fee_amount
         stats_data.liquidity_volume_y_out += at_price_acquire_y
         
         self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
         ############################################################################
            
            
      elif current_has_y:
         # in this branch, current point is same as those in its left, so form it into left range 
         self.current_point += 1
         self.sqrt_price_96 = self.sqrt_price_96 + mul_fraction_floor(self.sqrt_price_96, sqrt_rate_96() - pow_96(), pow_96())
      else:
         # only has liquidity_x part
         # TODO: seems this code is useless
         result.liquidity_x = self.liquidity_x

      if result.finished:
         return result

      if left_point < self.current_point:
         sqrt_price_l_96 = get_sqrt_price(left_point)
         x2y_range_comp_result = x_swap_y_range_complete(self.liquidity, sqrt_price_l_96, left_point, self.sqrt_price_96, self.current_point, amount_x)
         result.cost_x += x2y_range_comp_result.cost_x
         amount_x -= x2y_range_comp_result.cost_x
         result.acquire_y += x2y_range_comp_result.acquire_y
         if x2y_range_comp_result.complete_liquidity:
            result.finished = amount_x == 0
            result.final_pt = left_point
            result.sqrt_final_price_96 = sqrt_price_l_96
            result.liquidity_x = self.liquidity
            
            ############################################################################
            # update point_stats_data
            left_endpoint = result.final_pt // self.point_delta
            right_endpoint = self.current_point // self.point_delta
            
            token_x_in = 0
            token_y_out = 0

            if left_endpoint == right_endpoint:
               stats_data = self.point_info.get_point_stats_data_or_default(left_endpoint*self.point_delta)
               token_x_in = get_amount_x( self.liquidity, result.final_pt, self.current_point, get_sqrt_price(self.current_point), sqrt_rate_96(), True)
               token_y_out = get_amount_y( self.liquidity, get_sqrt_price(result.final_pt), get_sqrt_price(self.current_point), sqrt_rate_96(), False)
               
               # fee
               endpoint_fee_amount = mul_fraction_ceil(token_x_in, pool_fee, (10**6 - pool_fee))
               endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
               stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
               stats_data.p_fee_x += endpoint_charged_fee_amount
               
               stats_data.liquidity_volume_x_in += token_x_in + endpoint_fee_amount
               stats_data.liquidity_volume_y_out += token_y_out
               
               self.point_info.set_point_stats_data(left_endpoint*self.point_delta, stats_data)
            else:
               for i in range(left_endpoint, right_endpoint+1):
                  stats_data = self.point_info.get_point_stats_data_or_default(i*self.point_delta)

                  if i == left_endpoint:
                     #token_x_in = get_amount_x( liquidity: int, left_pt: int, right_pt: int, sqrt_price_r_96: int, sqrt_rate_96: int, upper: bool):
                     #token_y_out = get_amount_y( liquidity: int, sqrt_price_l_96: int, sqrt_price_r_96: int, sqrt_rate_96: int, upper: bool)
                     token_x_in = get_amount_x( self.liquidity, result.final_pt, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)
                     token_y_out = get_amount_y( self.liquidity, get_sqrt_price(result.final_pt), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)

                  elif i == right_endpoint:
                     token_x_in = get_amount_x( self.liquidity, i*self.point_delta, self.current_point, get_sqrt_price(self.current_point), sqrt_rate_96(), True)
                     token_y_out = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price(self.current_point), sqrt_rate_96(), False)
                     
                  else:
                     token_x_in = get_amount_x( self.liquidity, i*self.point_delta, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)
                     token_y_out = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)
                  
                  # fee
                  endpoint_fee_amount = mul_fraction_ceil(token_x_in, pool_fee, (10**6 - pool_fee))
                  endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
                  stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
                  stats_data.p_fee_x += endpoint_charged_fee_amount
                     
                  stats_data.liquidity_volume_x_in += token_x_in + endpoint_fee_amount
                  stats_data.liquidity_volume_y_out += token_y_out
 
                  self.point_info.set_point_stats_data(i*self.point_delta, stats_data)
            ############################################################################
         else:
            (at_price_cost_x, at_price_acquire_y, at_price_liquidity_x) = x_swap_y_at_price_liquidity(amount_x, x2y_range_comp_result.sqrt_loc_96, self.liquidity, 0)
            result.cost_x += at_price_cost_x
            result.acquire_y += at_price_acquire_y
            result.finished = True
            result.sqrt_final_price_96 = x2y_range_comp_result.sqrt_loc_96
            result.final_pt = x2y_range_comp_result.loc_pt
            result.liquidity_x = at_price_liquidity_x
            
            ############################################################################
            # update point_stats_data
            current_endpoint = result.final_pt // self.point_delta         

            stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)
            
            # fee
            endpoint_fee_amount = mul_fraction_ceil(at_price_cost_x, pool_fee, (10**6 - pool_fee))
            endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
            stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
            stats_data.p_fee_x += endpoint_charged_fee_amount
            
            stats_data.liquidity_volume_x_in += at_price_cost_x + endpoint_fee_amount
            stats_data.liquidity_volume_y_out += at_price_acquire_y
            
            self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
            ############################################################################

      else:
         result.final_pt = self.current_point
         result.sqrt_final_price_96 = self.sqrt_price_96


      return result

   # @param right_point: the right boundary of range
   # @param amount_y: the amount of token Y to swap-in
   # @return Y2XRangeRet
   # pub fn range_y_swap_x(&mut self, right_point: i32, amount_y: u128) -> Y2XRangeRet
   def range_y_swap_x(self, right_point: int, amount_y: int, pool_fee: int, protocol_fee_rate: int):
      result = Y2XRangeRet()
      # first, if current point is not all x, we can not move right directly
      start_has_y = self.liquidity_x < self.liquidity
      if start_has_y:
         (result.cost_y, result.acquire_x, result.liquidity_x) = y_swap_x_at_price_liquidity( amount_y, self.sqrt_price_96, self.liquidity_x )
         
         ############################################################################         
         # update point_stats_data
         current_endpoint = self.current_point // self.point_delta         

         stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)
         
         # fee
         endpoint_fee_amount = mul_fraction_ceil(result.cost_y, pool_fee, (10**6 - pool_fee))
         endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
         stats_data.fee_y += endpoint_fee_amount - endpoint_charged_fee_amount
         stats_data.p_fee_y += endpoint_charged_fee_amount
         
         stats_data.liquidity_volume_y_in += result.cost_y + endpoint_fee_amount
         stats_data.liquidity_volume_x_out += result.acquire_x
         
         self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
         ############################################################################
         
         if result.liquidity_x > 0 or result.cost_y >= amount_y:
            # it means remaining y is not enough to rise current price to price*1.0001
            # but y may remain, so we cannot simply use (cost_y == amount_y)
            result.finished = True
            result.final_pt = self.current_point
            result.sqrt_final_price_96 = self.sqrt_price_96
            return result
         else:
            # y not run out
            # not finsihed
            amount_y -= result.cost_y
            self.current_point += 1
            if self.current_point == right_point:
               result.final_pt = self.current_point
               # get fixed sqrt price to reduce accumulated error
               result.sqrt_final_price_96 = get_sqrt_price(right_point)
               return result
            # sqrt(price) + sqrt(price) * (1.0001 - 1) == sqrt(price) * 1.0001
            self.sqrt_price_96 = self.sqrt_price_96 + mul_fraction_floor(self.sqrt_price_96, sqrt_rate_96() - pow_96(), pow_96())

      sqrt_price_r_96 = get_sqrt_price(right_point)

      y2x_range_comp_result = y_swap_x_range_complete( self.liquidity, self.sqrt_price_96, self.current_point, sqrt_price_r_96, right_point, amount_y)

      result.cost_y += y2x_range_comp_result.cost_y
      amount_y -= y2x_range_comp_result.cost_y
      result.acquire_x += y2x_range_comp_result.acquire_x
      if y2x_range_comp_result.complete_liquidity:
         result.finished = amount_y == 0
         result.final_pt = right_point
         result.sqrt_final_price_96 = sqrt_price_r_96
         
         ############################################################################
         # update point_stats_data
         left_endpoint = self.current_point // self.point_delta
         right_endpoint = result.final_pt // self.point_delta
         
         token_y_in = 0
         token_x_out = 0
         
         if left_endpoint == right_endpoint:
            stats_data = self.point_info.get_point_stats_data_or_default(left_endpoint*self.point_delta)
            token_x_out = get_amount_x( self.liquidity, self.current_point, result.final_pt, get_sqrt_price(result.final_pt), sqrt_rate_96(), False)
            token_y_in = get_amount_y( self.liquidity, get_sqrt_price(self.current_point), get_sqrt_price(result.final_pt), sqrt_rate_96(), True)
            
            # fee
            endpoint_fee_amount = mul_fraction_ceil(token_y_in, pool_fee, (10**6 - pool_fee))
            endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
            stats_data.fee_y += endpoint_charged_fee_amount
            
            stats_data.liquidity_volume_y_in += token_y_in + endpoint_fee_amount
            stats_data.liquidity_volume_x_out += token_x_out
            
            self.point_info.set_point_stats_data(left_endpoint*self.point_delta, stats_data)
         else:
            for i in range(left_endpoint, right_endpoint+1):
               stats_data = self.point_info.get_point_stats_data_or_default(i*self.point_delta)

               if i == left_endpoint:
                  token_x_out = get_amount_x( self.liquidity, self.current_point, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)
                  token_y_in = get_amount_y( self.liquidity, get_sqrt_price(self.current_point), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)

               elif i == right_endpoint:
                  token_x_out = get_amount_x( self.liquidity, i*self.point_delta, result.final_pt, get_sqrt_price(result.final_pt), sqrt_rate_96(), False)
                  token_y_in = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price(result.final_pt), sqrt_rate_96(), True)
                  
               else:
                  token_x_out = get_amount_x( self.liquidity, i*self.point_delta, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)
                  token_y_in = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)
               
               # fee
               endpoint_fee_amount = mul_fraction_ceil(token_y_in, pool_fee, (10**6 - pool_fee))
               endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
               stats_data.fee_y += endpoint_fee_amount - endpoint_charged_fee_amount
               stats_data.p_fee_y += endpoint_charged_fee_amount
                  
               stats_data.liquidity_volume_y_in += token_y_in + endpoint_fee_amount
               stats_data.liquidity_volume_x_out += token_x_out
               
               self.point_info.set_point_stats_data(i*self.point_delta, stats_data)
         ############################################################################
         
      else:
         # trade at loc_pt
         (loc_cost_y, loc_acquire_x, loc_liquidity_x) = y_swap_x_at_price_liquidity(amount_y, y2x_range_comp_result.sqrt_loc_96, self.liquidity)

         result.liquidity_x = loc_liquidity_x
         result.cost_y += loc_cost_y
         result.acquire_x += loc_acquire_x
         result.finished = True
         result.sqrt_final_price_96 = y2x_range_comp_result.sqrt_loc_96
         result.final_pt = y2x_range_comp_result.loc_pt
         
         ############################################################################         
         # update point_stats_data
         current_endpoint = y2x_range_comp_result.loc_pt // self.point_delta         

         stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)
         
         # fee
         endpoint_fee_amount = mul_fraction_ceil(loc_cost_y, pool_fee, (10**6 - pool_fee))
         endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
         stats_data.fee_y += endpoint_fee_amount - endpoint_charged_fee_amount
         stats_data.p_fee_y += endpoint_charged_fee_amount
         
         stats_data.liquidity_volume_y_in += loc_cost_y + endpoint_fee_amount
         stats_data.liquidity_volume_x_out += loc_acquire_x
         
         self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
         ############################################################################
      return result


   # @param left_point: the left boundary of range
   # @param desire_y: the amount of token Y to swap-out
   # @return X2YRangeRetDesire
   # range_x_swap_y_desire(&mut self, left_point: i32, desire_y: u128) -> X2YRangeRetDesire
   def range_x_swap_y_desire(self, left_point: int, desire_y: int, pool_fee: int, protocol_fee_rate: int):
      result = X2YRangeRetDesire()
      current_has_y = self.liquidity_x < self.liquidity

      if current_has_y and (self.liquidity_x > 0 or left_point == self.current_point):
         (result.cost_x, result.acquire_y, result.liquidity_x) = x_swap_y_at_price_liquidity_desire( desire_y, self.sqrt_price_96, self.liquidity, self.liquidity_x )

         if result.liquidity_x < self.liquidity or result.acquire_y >= desire_y:
            result.finished = True
            result.final_pt = self.current_point
            result.sqrt_final_price_96 = self.sqrt_price_96
         else:
            desire_y -= result.acquire_y

         ############################################################################         
         # update point_stats_data
         current_endpoint = self.current_point // self.point_delta         

         stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)

         
         # fee
         endpoint_fee_amount = mul_fraction_ceil(result.cost_x, pool_fee, (10**6 - pool_fee))
         endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
         stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
         stats_data.p_fee_x += endpoint_charged_fee_amount
         
         stats_data.liquidity_volume_x_in += result.cost_x + endpoint_fee_amount
         stats_data.liquidity_volume_y_out += result.acquire_y
         
         self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
         ############################################################################

      elif current_has_y: # all y
         self.current_point += 1
         self.sqrt_price_96 = self.sqrt_price_96 + mul_fraction_floor(self.sqrt_price_96, sqrt_rate_96() - pow_96(), pow_96())
      else:
         result.liquidity_x = self.liquidity_x
      
      if result.finished:
         return result

      if left_point < self.current_point:
         sqrt_price_l_96 = get_sqrt_price(left_point)
         x2y_range_comp_desire_result = x_swap_y_range_complete_desire( self.liquidity, sqrt_price_l_96, left_point, self.sqrt_price_96, self.current_point, desire_y )            
         result.cost_x += x2y_range_comp_desire_result.cost_x
         desire_y -= x2y_range_comp_desire_result.acquire_y
         result.acquire_y += x2y_range_comp_desire_result.acquire_y
         if x2y_range_comp_desire_result.complete_liquidity:
            result.finished = desire_y == 0
            result.final_pt = left_point
            result.sqrt_final_price_96 = sqrt_price_l_96
            result.liquidity_x = self.liquidity
            
            ############################################################################
            # update point_stats_data
            left_endpoint = result.final_pt // self.point_delta
            right_endpoint = self.current_point // self.point_delta
            
            token_x_in = 0
            token_y_out = 0

            if left_endpoint == right_endpoint:
               stats_data = self.point_info.get_point_stats_data_or_default(left_endpoint*self.point_delta)
               token_x_in = get_amount_x( self.liquidity, result.final_pt, self.current_point, get_sqrt_price(self.current_point), sqrt_rate_96(), True)
               token_y_out = get_amount_y( self.liquidity, get_sqrt_price(result.final_pt), get_sqrt_price(self.current_point), sqrt_rate_96(), False)
               
               # fee
               endpoint_fee_amount = mul_fraction_ceil(token_x_in, pool_fee, (10**6 - pool_fee))
               endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
               stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
               stats_data.p_fee_x += endpoint_charged_fee_amount
               
               stats_data.liquidity_volume_x_in += token_x_in + endpoint_fee_amount
               stats_data.liquidity_volume_y_out += token_y_out
               
               self.point_info.set_point_stats_data(left_endpoint*self.point_delta, stats_data)
            else:
               for i in range(left_endpoint, right_endpoint+1):
                  stats_data = self.point_info.get_point_stats_data_or_default(i*self.point_delta)

                  if i == left_endpoint:
                     token_x_in = get_amount_x( self.liquidity, result.final_pt, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)
                     token_y_out = get_amount_y( self.liquidity, get_sqrt_price(result.final_pt), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)

                  elif i == right_endpoint:
                     token_x_in = get_amount_x( self.liquidity, i*self.point_delta, self.current_point, get_sqrt_price(self.current_point), sqrt_rate_96(), True)
                     token_y_out = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price(self.current_point), sqrt_rate_96(), False)
                     
                  else:
                     token_x_in = get_amount_x( self.liquidity, i*self.point_delta, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)
                     token_y_out = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)
                  
                  # fee
                  endpoint_fee_amount = mul_fraction_ceil(token_x_in, pool_fee, (10**6 - pool_fee))
                  endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
                  stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
                  stats_data.p_fee_x += endpoint_charged_fee_amount
                     
                  stats_data.liquidity_volume_x_in += token_x_in + endpoint_fee_amount
                  stats_data.liquidity_volume_y_out += token_y_out
                  
                  self.point_info.set_point_stats_data(i*self.point_delta, stats_data)
            ############################################################################            
            
         else:
            (loc_cost_x, loc_acquire_y, new_liquidity_x) = x_swap_y_at_price_liquidity_desire(desire_y, x2y_range_comp_desire_result.sqrt_loc_96, self.liquidity, 0 )
            result.liquidity_x = new_liquidity_x
            result.cost_x += loc_cost_x
            result.acquire_y += loc_acquire_y
            result.finished = True
            result.sqrt_final_price_96 = x2y_range_comp_desire_result.sqrt_loc_96
            result.final_pt = x2y_range_comp_desire_result.loc_pt

            ############################################################################
            # update point_stats_data
            current_endpoint = result.final_pt // self.point_delta         

            stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)
            
            # fee
            endpoint_fee_amount = mul_fraction_ceil(loc_cost_x, pool_fee, (10**6 - pool_fee))
            endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
            stats_data.fee_x += endpoint_fee_amount - endpoint_charged_fee_amount
            stats_data.p_fee_x += endpoint_charged_fee_amount
            
            stats_data.liquidity_volume_x_in += loc_cost_x + endpoint_fee_amount
            stats_data.liquidity_volume_y_out += loc_acquire_y
            
            self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
            ############################################################################

      else:
         result.final_pt = self.current_point
         result.sqrt_final_price_96 = self.sqrt_price_96

      return result


   # @param right_point: the right boundary of range
   # @param desire_x: the amount of token X to swap-out
   # @return X2YRangeRetDesire
   # pub fn range_y_swap_x_desire(&mut self, right_point: i32, desire_x: u128) -> Y2XRangeRetDesire
   def range_y_swap_x_desire(self, right_point: int, desire_x: int, pool_fee: int, protocol_fee_rate: int):
      result = Y2XRangeRetDesire()
      start_has_y = self.liquidity_x < self.liquidity
      if start_has_y:
         (result.cost_y, result.acquire_x, result.liquidity_x) = y_swap_x_at_price_liquidity_desire(desire_x, self.sqrt_price_96, self.liquidity_x)

         ############################################################################         
         # update point_stats_data
         current_endpoint = self.current_point // self.point_delta         

         stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)
         
         # fee
         endpoint_fee_amount = mul_fraction_ceil(result.cost_y, pool_fee, (10**6 - pool_fee))
         endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
         stats_data.fee_y += endpoint_fee_amount - endpoint_charged_fee_amount
         stats_data.p_fee_y += endpoint_charged_fee_amount
         
         stats_data.liquidity_volume_y_in += result.cost_y + endpoint_fee_amount
         stats_data.liquidity_volume_x_out += result.acquire_x
         
         self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
         ############################################################################         
         
         if result.liquidity_x > 0 or result.acquire_x >= desire_x:
            # currX remain, means desire runout
            result.finished = True
            result.final_pt = self.current_point
            result.sqrt_final_price_96 = self.sqrt_price_96
            return result
         else:
            # not finished
            desire_x -= result.acquire_x
            self.current_point += 1
            if self.current_point == right_point:
               result.final_pt = self.current_point
               # get fixed sqrt price to reduce accumulated error
               result.sqrt_final_price_96 = get_sqrt_price(right_point)
               return result

            # sqrt(price) + sqrt(price) * (1.0001 - 1) == sqrt(price) * 1.0001
            self.sqrt_price_96 = self.sqrt_price_96 + mul_fraction_floor(self.sqrt_price_96, sqrt_rate_96() - pow_96(), pow_96())


      sqrt_price_r_96 = get_sqrt_price(right_point)
      y2x_range_comp_desire_result = y_swap_x_range_complete_desire( self.liquidity, self.sqrt_price_96, self.current_point, sqrt_price_r_96, right_point, desire_x )

      result.cost_y += y2x_range_comp_desire_result.cost_y
      result.acquire_x += y2x_range_comp_desire_result.acquire_x
      desire_x -= y2x_range_comp_desire_result.acquire_x

      if y2x_range_comp_desire_result.complete_liquidity:
         result.finished = desire_x == 0
         result.final_pt = right_point
         result.sqrt_final_price_96 = sqrt_price_r_96
         
         ############################################################################
         # update point_stats_data
         left_endpoint = self.current_point // self.point_delta
         right_endpoint = result.final_pt // self.point_delta
         
         token_y_in = 0
         token_x_out = 0
         
         if left_endpoint == right_endpoint:
            stats_data = self.point_info.get_point_stats_data_or_default(left_endpoint*self.point_delta)
            token_x_out = get_amount_x( self.liquidity, self.current_point, result.final_pt, get_sqrt_price(result.final_pt), sqrt_rate_96(), False)
            token_y_in = get_amount_y( self.liquidity, get_sqrt_price(self.current_point), get_sqrt_price(result.final_pt), sqrt_rate_96(), True)
            
            # fee
            endpoint_fee_amount = mul_fraction_ceil(token_y_in, pool_fee, (10**6 - pool_fee))
            endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
            stats_data.fee_y += endpoint_fee_amount - endpoint_charged_fee_amount
            stats_data.p_fee_y += endpoint_charged_fee_amount
            
            stats_data.liquidity_volume_y_in += token_y_in + endpoint_fee_amount
            stats_data.liquidity_volume_x_out += token_x_out
            
            self.point_info.set_point_stats_data(left_endpoint*self.point_delta, stats_data)
         else:
            for i in range(left_endpoint, right_endpoint+1):
               stats_data = self.point_info.get_point_stats_data_or_default(i*self.point_delta)

               if i == left_endpoint:
                  token_x_out = get_amount_x( self.liquidity, self.current_point, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)
                  token_y_in = get_amount_y( self.liquidity, get_sqrt_price(self.current_point), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)

               elif i == right_endpoint:
                  token_x_out = get_amount_x( self.liquidity, i*self.point_delta, result.final_pt, get_sqrt_price(result.final_pt), sqrt_rate_96(), False)
                  token_y_in = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price(result.final_pt), sqrt_rate_96(), True)
                  
               else:
                  token_x_out = get_amount_x( self.liquidity, i*self.point_delta, (i+1)*self.point_delta, get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), False)
                  token_y_in = get_amount_y( self.liquidity, get_sqrt_price(i*self.point_delta), get_sqrt_price((i+1)*self.point_delta), sqrt_rate_96(), True)
               
               # fee
               endpoint_fee_amount = mul_fraction_ceil(token_y_in, pool_fee, (10**6 - pool_fee))
               endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
               stats_data.fee_y += endpoint_fee_amount - endpoint_charged_fee_amount
               stats_data.p_fee_y += endpoint_charged_fee_amount
                  
               stats_data.liquidity_volume_y_in += token_y_in + endpoint_fee_amount
               stats_data.liquidity_volume_x_out += token_x_out
               
               self.point_info.set_point_stats_data(i*self.point_delta, stats_data)
         ############################################################################
         
      else:
         (loc_cost_y, loc_acquire_x, new_liquidity_x) = y_swap_x_at_price_liquidity_desire(desire_x, y2x_range_comp_desire_result.sqrt_loc_96, self.liquidity)
         result.liquidity_x = new_liquidity_x
         result.cost_y += loc_cost_y
         result.acquire_x += loc_acquire_x
         result.finished = True
         result.final_pt = y2x_range_comp_desire_result.loc_pt
         result.sqrt_final_price_96 = y2x_range_comp_desire_result.sqrt_loc_96

         ############################################################################         
         # update point_stats_data
         current_endpoint = y2x_range_comp_result.loc_pt // self.point_delta         

         stats_data = self.point_info.get_point_stats_data_or_default(current_endpoint*self.point_delta)
         
         # fee
         endpoint_fee_amount = mul_fraction_ceil(loc_cost_y, pool_fee, (10**6 - pool_fee))
         endpoint_charged_fee_amount = endpoint_fee_amount * protocol_fee_rate // BP_DENOM
         stats_data.fee_y += endpoint_fee_amount - endpoint_charged_fee_amount
         stats_data.p_fee_y += endpoint_charged_fee_amount

         stats_data.liquidity_volume_y_in += loc_cost_y + endpoint_fee_amount
         stats_data.liquidity_volume_x_out += loc_acquire_x
         
         self.point_info.set_point_stats_data(current_endpoint*self.point_delta, stats_data)
         ############################################################################

      return result

   #  Update the liquidity and fee when passing the endpoint
   #  @param point: endpoint of id
   #  @param is_quote: whether the quote function is calling
   #  @param to_the_left: whether to cross the point from right to left
   # def pass_endpoint(self, point: int, is_quote: bool, to_the_left: bool)
   def pass_endpoint(self, point: int, is_quote: bool, to_the_left: bool):
      #point_data = self.point_info.data[point]
      #point_data = self.point_info.get_order_data(point)
      liquidity_data = self.point_info.get_liquidity_data(point)
      if liquidity_data == None:
         liquidity_data = LiquidityData()

      liquidity_data.pass_endpoint(self.fee_scale_x_128, self.fee_scale_y_128)
      liquidity_delta = 0
      if to_the_left:
         liquidity_delta = -liquidity_data.liquidity_delta
      else:
         liquidity_delta = liquidity_data.liquidity_delta

      self.liquidity = liquidity_add_delta(self.liquidity, liquidity_delta)

      if is_quote == False:
         #point_data.liquidity_data = liquidity_data
         #self.point_info.data[point] = point_data
         self.point_info.set_liquidity_data(point, liquidity_data)

   # After swap order, update point_info and slot_bitmap at the current point
   # @param point_data: point_data at the current point
   # @param order_data: order_data at the current point
   # @param is_quote: whether the quote function is calling
   def update_point_order(self, point_data: PointData, order_data: OrderData, is_quote: bool):
      if is_quote == False:
         point_data.order_data = order_data
         if False == point_data.has_active_order() and False == point_data.has_active_liquidity():
            self.slot_bitmap.set_zero(self.current_point, self.point_delta)
         self.point_info.data[self.current_point] = point_data


   # pub fn get_pool_fee_by_user(&self, vip_info: &Option<HashMap<PoolId, u32>>) -> u32
   def get_pool_fee_by_user(self, vip_info):
      if self.pool_id in vip_info:
         return self.fee * vip_info[self.pool_id] // BP_DENOM
      else:
         return self.fee

# Calculate the new liquidity by the current liquidity and the change in liquidity
# @param liquidity
# @param delta
# @return new liquidity
def liquidity_add_delta(liquidity: int, delta: int):
   if delta < 0:
      return liquidity - (-delta)
   else:
      return liquidity + delta


def gen_pool_id( token_a: str, token_b: str, fee: int ):
   return token_a+POOL_ID_BREAK+token_b+POOL_ID_BREAK+f"{fee}"

def parse_pool_id( pool_id ):
   a = pool_id.split(POOL_ID_BREAK,2)
   token_x = a[0]
   token_y = a[1]
   fee = int(a[2])
   return (token_x, token_y, fee)

#fn range_info_to_the_left_of_cp( pool: &Pool, left_point: i32, right_point: i32, ret: &mut HashMap<i32, RangeInfo>)
def range_info_to_the_left_of_cp( pool: Pool, left_point: int, right_point: int, ret ):
   liquidity = pool.liquidity
   if left_point != pool.current_point:
      current_point = pool.slot_bitmap.get_nearest_right_valued_slot(pool.current_point, pool.point_delta, left_point / pool.point_delta)
      
      if current_point != None:
         while current_point < left_point:
            if pool.point_info.is_endpoint(current_point, pool.point_delta):
               liquidity_data = pool.point_info.get_liquidity_data(current_point)
               if liquidity_data.liquidity_delta > 0:
                  liquidity += liquidity_data.liquidity_delta
               else:
                  liquidity -= (-liquidity_data.liquidity_delta)
            point = pool.slot_bitmap.get_nearest_right_valued_slot(current_point, pool.point_delta, left_point / pool.point_delta)
            if point is None:
               break
            else:
               current_point = point

   current_point = left_point
   range_left_point = left_point
   while current_point < right_point:
      range_right_point = right_point
      point = pool.slot_bitmap.get_nearest_right_valued_slot(current_point, pool.point_delta, right_point / pool.point_delta)
      if point != None:
         range_right_point = point

      if pool.point_info.is_endpoint(range_right_point, pool.point_delta):
         if range_left_point != left_point:
               liquidity_data = pool.point_info.get_liquidity_data(range_left_point)
               if liquidity_data.liquidity_delta > 0:
                  liquidity += liquidity_data.liquidity_delta
               else:
                  liquidity -= (-liquidity_data.liquidity_delta)

         range_info = RangeInfo()
         range_info.left_point = range_left_point
         if range_right_point < right_point:
            range_info.right_point = range_right_point
         else:
            range_info.right_point = right_point
         range_info.amount_l = liquidity
         
         ret[range_left_point] = range_info
         
         range_left_point = range_right_point
      elif range_right_point == right_point:
         if range_left_point != left_point:
               liquidity_data = pool.point_info.get_liquidity_data(range_left_point)
               if liquidity_data.liquidity_delta > 0:
                  liquidity += liquidity_data.liquidity_delta
               else:
                  liquidity -= (-liquidity_data.liquidity_delta)

         range_info = RangeInfo()
         range_info.left_point = range_left_point
         range_info.right_point = right_point
         range_info.amount_l = liquidity
         
         ret[range_left_point] = range_info

      current_point = range_right_point


#fn range_info_to_the_right_of_cp( pool: &Pool, left_point: i32, right_point: i32, ret: &mut HashMap<i32, RangeInfo>)
def range_info_to_the_right_of_cp( pool: Pool, left_point: int, right_point: int, ret: {}):
   liquidity = pool.liquidity
   if pool.point_info.is_endpoint(pool.current_point, pool.point_delta):
      liquidity_data = pool.point_info.get_liquidity_data(pool.current_point)
      if liquidity_data.liquidity_delta > 0:
         liquidity -= liquidity_data.liquidity_delta
      else:
         liquidity += (-liquidity_data.liquidity_delta)

   if right_point != pool.current_point:
      current_point = pool.slot_bitmap.get_nearest_left_valued_slot(pool.current_point - 1, pool.point_delta, right_point / pool.point_delta)
      if current_point != None:
         while current_point > right_point:
            if pool.point_info.is_endpoint(current_point, pool.point_delta):
               liquidity_data = pool.point_info.get_liquidity_data(current_point)
               if liquidity_data.liquidity_delta > 0:
                  liquidity -= liquidity_data.liquidity_delta
               else:
                  liquidity += (-liquidity_data.liquidity_delta)

            point = pool.slot_bitmap.get_nearest_left_valued_slot(current_point - 1, pool.point_delta, right_point / pool.point_delta)
            if point is None:
               break
            else:
               current_point = point
      
   current_point = right_point
   range_right_point = right_point
   while current_point > left_point:
      range_left_point = left_point
      point = pool.slot_bitmap.get_nearest_left_valued_slot(current_point - 1, pool.point_delta, left_point / pool.point_delta)
      if point != None:
         range_left_point = point
      
      if pool.point_info.is_endpoint(range_left_point, pool.point_delta):
         if range_right_point != right_point:
            liquidity_data = pool.point_info.get_liquidity_data(range_right_point)
            if liquidity_data.liquidity_delta > 0:
               liquidity -= liquidity_data.liquidity_delta
            else:
               liquidity += (-liquidity_data.liquidity_delta)

         range_info = RangeInfo()

         if range_left_point > left_point:
            range_info.left_point = range_left_point
         else:
            range_info.left_point = left_point
            
         range_info.right_point = range_right_point
         range_info.amount_l = liquidity
         
         ret[range_left_point] = range_info

         range_right_point = range_left_point
      elif range_left_point == left_point:
         if range_right_point != right_point:
            liquidity_data = pool.point_info.get_liquidity_data(range_right_point)
            if liquidity_data.liquidity_delta > 0:
               liquidity -= liquidity_data.liquidity_delta
            else:
               liquidity += (-liquidity_data.liquidity_delta)

         range_info = RangeInfo()

         range_info.left_point = left_point
         range_info.right_point = range_right_point
         range_info.amount_l = liquidity
         
         ret[left_point] = range_info

      current_point = range_left_point



'''
pub pools: UnorderedMap<PoolId, VPool>,
pub users: LookupMap<AccountId, VUser>,
pub user_liquidities: LookupMap<LptId, UserLiquidity>,
// required by approval extension
pub approvals_by_id: LookupMap<LptId, HashMap<AccountId, u64>>,
pub next_approval_id_by_id: LookupMap<LptId, u64>,
pub user_orders: LookupMap<OrderId, UserOrder>,   
'''
MARKET_QUERY_SLOT_LIMIT = 150000
class Dcl:
   def __init__(self, protocol_fee_rate = 2000, name = ""):
      self.name = name
      self.protocol_fee_rate = protocol_fee_rate
      self.fee_tier = {'100': 1, '400': 8, '2000': 40, '10000': 200}
      self.pools = {} # Pool list
      self.user_liquidities = {} # user liquidities. LookupMap<LptId, VUserLiquidity>
      self.user_orders = {} # UserOrder. LookupMap<OrderId, VUserOrder>
      self.users = {} # User. Will register user automatically
      self.vip_users = {} # vip users
      self.mft_supply = {}
      self.latest_liquidity_id = 0
      self.latest_order_id = 0
      self.liquidity_count = 0
      self.state = RUNNING
      
      # state files
      self.dcl_root = None
      self.dcl_pool = None
      self.dcl_user_liquidities = None
      self.dcl_user_orders = None
      self.dcl_pointinfo = None
      self.dcl_slotbitmap = None
      self.dcl_vip_users = None

   def set_instance_name(self, name =""):
      self.name = name
      for k in self.pools:
         self.pools[k].parent_name = name
   
   def dump(self):
      print("----------------dump dcl: "+self.name+"-------------------")
      print("protocol_fee_rate:",self.protocol_fee_rate)
      print("fee_tier:",self.fee_tier)
      print("latest_liquidity_id:",self.latest_liquidity_id)
      print("latest_order_id:",self.latest_order_id)
      print("liquidity_count:",self.liquidity_count)
      print("----------------dump dcl: "+self.name+" completed-------------------")

   def load_dcl_state(self):
      # read data from file
      self.dcl_root = OpenFile("./dcl_root.json")
      self.dcl_pool = OpenFile("./dcl_pool.json")
      self.dcl_user_liquidities = OpenFile("./dcl_user_liquidities.json")
      self.dcl_user_orders = OpenFile("./dcl_user_orders.json")
      self.dcl_pointinfo = OpenFile("./dcl_pointinfo.json")
      self.dcl_slotbitmap = OpenFile("./dcl_slotbitmap.json")
      self.dcl_vip_users = OpenFile("./dcl_vip_users.json")
      
      # load state data into structure
      self.latest_liquidity_id = self.dcl_root.get('latest_liquidity_id',0)
      self.latest_order_id = self.dcl_root.get('latest_order_id',0)
      self.load_pool()
      self.load_user_liquidities()
      self.load_user_limit_orders()
      self.load_vip_users()
      

   def load_pool(self):
      for pool_data in self.dcl_pool.values():
         pool = Pool()
         
         pool.pool_id = pool_data['pool_id']
         pool.token_x = pool_data['token_x']
         pool.token_y = pool_data['token_y']
         pool.fee = pool_data['fee']
         pool.point_delta = pool_data['point_delta']
         pool.current_point = pool_data['current_point']
         pool.sqrt_price_96 = pool_data['sqrt_price_96']
         pool.liquidity = pool_data['liquidity']
         pool.liquidity_x = pool_data['liquidity_x']
         pool.max_liquidity_per_point = pool_data['max_liquidity_per_point']
         pool.fee_scale_x_128 = pool_data['fee_scale_x_128']
         pool.fee_scale_y_128 = pool_data['fee_scale_y_128']
         pool.total_fee_x_charged = pool_data['total_fee_x_charged']
         pool.total_fee_y_charged = pool_data['total_fee_y_charged']
         pool.volume_x_in = pool_data['volume_x_in']
         pool.volume_y_in = pool_data['volume_y_in']
         pool.volume_x_out = pool_data['volume_x_out']
         pool.volume_y_out = pool_data['volume_y_out']
         pool.total_liquidity = pool_data['total_liquidity']
         pool.total_order_x = pool_data['total_order_x']
         pool.total_order_y = pool_data['total_order_y']
         pool.total_x = pool_data['total_x']
         pool.total_y = pool_data['total_y']
         pool.RunningState = pool_data['RunningState']
         pool.point_info = PointInfo()
         pool.point_info.load_point_info(self.dcl_pointinfo[pool_data['pool_id']])
         pool.slot_bitmap = Slot_BitMap()
         pool.slot_bitmap.load_slot_bitmap(self.dcl_slotbitmap[pool_data['pool_id']])
         #pool.slot_bitmap.dump()
         token_x_meta = get_ft_metadata(pool.token_x)
         token_y_meta = get_ft_metadata(pool.token_y)
         pool.token_x_decimal = token_x_meta['decimals']
         pool.token_y_decimal = token_y_meta['decimals']
         
         print("load pool: ", pool_data['pool_id'], ", current_point =", pool_data['current_point'])

         self.pools[pool_data['pool_id']] = pool

   def load_user_liquidities(self):
      for user_liquidity in self.dcl_user_liquidities.values():
         for lptid, liquidity in user_liquidity.items():
            userliquidity = UserLiquidity()
            userliquidity.LptId = liquidity['LptId']
            userliquidity.owner_id = liquidity['owner_id']
            userliquidity.pool_id = liquidity['pool_id']
            userliquidity.left_point = liquidity['left_point']
            userliquidity.right_point = liquidity['right_point']
            userliquidity.last_fee_scale_x_128 = liquidity['last_fee_scale_x_128']
            userliquidity.last_fee_scale_y_128 = liquidity['last_fee_scale_y_128']
            userliquidity.amount = liquidity['amount']
            if 'mft_id' in liquidity:
               userliquidity.mft_id = liquidity['mft_id']
            if 'v_liquidity' in liquidity:
               userliquidity.v_liquidity = liquidity['v_liquidity']
            userliquidity.unclaimed_fee_x = liquidity['unclaimed_fee_x']
            userliquidity.unclaimed_fee_y = liquidity['unclaimed_fee_y']

            self.user_liquidities[lptid] = userliquidity


   def load_user_limit_orders(self):
      for user_order in self.dcl_user_orders.values():
         for order_id, order in user_order.items():
            userorder = UserOrder()
            userorder.order_id = order['order_id']
            userorder.owner_id = order['owner_id']
            userorder.pool_id = order['pool_id']
            userorder.point = order['point']
            userorder.sell_token = order['sell_token']
            userorder.buy_token = order['buy_token']
            userorder.original_deposit_amount = order['original_deposit_amount']
            userorder.swap_earn_amount = order['swap_earn_amount']
            userorder.original_amount = order['original_amount']
            userorder.cancel_amount = order['cancel_amount']
            userorder.created_at = order['created_at']
            userorder.last_acc_earn = order['last_acc_earn']
            userorder.remain_amount = order['remain_amount']
            userorder.bought_amount = order['bought_amount']
            userorder.unclaimed_amount = order['unclaimed_amount']

            self.user_orders[order_id] = userorder


      
   def load_vip_users(self):
      self.vip_users = copy.deepcopy(self.dcl_vip_users)
      pass


   def dump_pools_stats_data(self):
      stats_result = {}

      for pool_id in self.pools.keys():
         current_point = self.pools[pool_id].current_point
         point_delta = self.pools[pool_id].point_delta
         pool_fee = self.pools[pool_id].fee
         token_x_decimal = self.pools[pool_id].token_x_decimal
         token_y_decimal = self.pools[pool_id].token_y_decimal
         stats_result[pool_id] = self.pools[pool_id].point_info.dump_stats_data(current_point, point_delta, pool_fee, self.protocol_fee_rate, token_x_decimal, token_y_decimal)
      
      filepath = './dcl_endpoint_stats.json'
      with open(filepath, mode='w', encoding="utf-8") as f:
         json.dump(stats_result, f, sort_keys = True)
         print("%s saved" % filepath)

   def pause_contract(self):
      self.state = PAUSED

   def resume_contract(self):
      self.state = RUNNING
   
   def assert_contract_running(self):
      return (self.state == RUNNING)

   def create_pool(self, token_a: str, token_b: str, fee: int, init_point: int ):
      pool_id = gen_pool_id(token_a, token_b, fee)
      #pool_id = PoolId::gen_from(&token_a, &token_b, fee)
      #require!( self.internal_get_pool(&pool_id).is_none(), E405_POOL_ALREADY_EXIST)
      #self.internal_set_pool( &pool_id, Pool::new( &pool_id, *self.data().fee_tier.get(&fee).expect(E402_ILLEGAL_FEE), init_point ))
      pool = Pool(self.name)
      pool.pool_id = pool_id
      pool.token_x = token_a
      pool.token_y = token_b
      pool.current_point = init_point
      pool.point_delta = self.fee_tier[f"{fee}"]

      pool.current_point = init_point
      pool.sqrt_price_96 = get_sqrt_price(init_point)
      pool.fee = fee

      point_num = ((RIGHT_MOST_POINT - LEFT_MOST_POINT) // pool.point_delta ) + 1
      
      pool.max_liquidity_per_point = ((1<<128) - 1) // point_num
      
      print("create_pool: point_num =", point_num)

      self.pools[pool_id] = pool


   # Get Pool from pool_id
   # @param pool_id
   # @return Option<Pool>
   def get_pool(self, pool_id: str):
      if pool_id in self.pools.keys():
         return self.pools[pool_id]
      else:
         return None

   def set_pool(self, pool_id: str, pool):
      self.pools[pool_id] = pool

   def get_user_order(self, order_id: str):
      if order_id in self.user_orders.keys():
         return self.user_orders[order_id]
      else:
         print("order_id : "+order_id)
         print("E304_ORDER_NOT_FOUND : "+self.name)
         raise Exception("E304_ORDER_NOT_FOUND")
         return None

   def set_user_order(self, order_id: str, order):
      self.user_orders[order_id] = order
      
   def get_user(self, user_id: str):
      if user_id not in self.users.keys():
         self.users[user_id] = User()
      return self.users[user_id]

   def set_user(self, user_id: str, user):
      self.users[user_id] = user

   def gen_lpt_id(self, pool_id: str):
      self.latest_liquidity_id += 1
      return pool_id+LPT_ID_BREAK+str(self.latest_liquidity_id)

   def get_user_liquidity(self, lpt_id: str):
      if lpt_id not in self.user_liquidities.keys():
         print("Error: E207_LIQUIDITY_NOT_FOUND : "+self.name)
         raise Exception("lpt_id not in self.user_liquidities.keys()")
         return None
      else:
         return self.user_liquidities[lpt_id]

   def set_user_liquidity(self, lpt_id: str, user_liquidity: UserLiquidity):
      self.user_liquidities[lpt_id] = user_liquidity


   def gen_order_id(self, pool_id: str):
      self.latest_order_id += 1
      return pool_id+ORDER_ID_BREAK+str(self.latest_order_id)

   def internal_mint_liquidity(self, user: User, liquidity: UserLiquidity):
      #user.liquidity_keys.insert(&liquidity.lpt_id);
      self.user_liquidities[liquidity.LptId] = liquidity
      self.liquidity_count += 1
      self.set_user(user.user_id, user) 


   def internal_burn_liquidity(self, user: User, liquidity: UserLiquidity):
      self.user_liquidities.pop(liquidity.LptId, None)
      self.liquidity_count -= 1
      self.set_user(user.user_id, user)


   # If user has a LPT with same pool_id&pl&pr, 
   # it is an increase opertaion, else mint.
   # cause there is a UnorederMap<pool_id:lp:rp, lptid> per user.
   # @param pool_id: a string like token_a|token_b|fee
   # @param left_point: left point of this range
   # @param right_point: right point of this range
   # @param amount_x: the number of token X users expect to add liquidity to use
   # @param amount_y: the number of token Y users expect to add liquidity to use
   # @param min_amount_x: the minimum number of token X users expect to add liquidity to use
   # @param min_amount_y: the minimum number of token Y users expect to add liquidity to use
   # @return the exist or new-mint lp token id, a string like pool_id|inner_id
   # add_liquidity( &mut self, pool_id: PoolId, left_point: i32, right_point: i32, amount_x: U128, amount_y: U128, min_amount_x: U128, min_amount_y: U128 ) -> LptId
   def add_liquidity(self, user_id, pool_id: str, left_point: int, right_point: int, amount_x: int, amount_y: int, min_amount_x: int, min_amount_y: int ):
      user = self.get_user(user_id)
      pool = self.get_pool(pool_id)
      if pool is None:
         print("Invalid pool_id")
         return None
      if (left_point % pool.point_delta) != 0 and (right_point % pool.point_delta) != 0:
         print("E200_INVALID_ENDPOINT")
         return None
      if right_point <= left_point:
         print("E202_ILLEGAL_POINT")
         return None
      if right_point - left_point >= RIGHT_MOST_POINT:
         print("E202_ILLEGAL_POINT")
         return None
      if left_point < LEFT_MOST_POINT and right_point > RIGHT_MOST_POINT:
         print("E202_ILLEGAL_POINT")
         return None
      
      (new_liquidity, need_x, need_y, acc_fee_x_in_128, acc_fee_y_in_128) = pool.internal_add_liquidity(left_point, right_point, amount_x, amount_y, min_amount_x, min_amount_y)

      lpt_id = self.gen_lpt_id(pool_id)
      
      liquidity = UserLiquidity()
      liquidity.LptId = lpt_id
      liquidity.owner_id = user_id
      liquidity.pool_id = pool_id
      liquidity.left_point = left_point
      liquidity.right_point =right_point
      liquidity.last_fee_scale_x_128 = acc_fee_x_in_128
      liquidity.last_fee_scale_y_128 = acc_fee_y_in_128
      liquidity.amount = new_liquidity
      liquidity.unclaimed_fee_x = None
      liquidity.unclaimed_fee_y = None
      
      pool.total_liquidity += new_liquidity
      pool.total_x += need_x
      pool.total_y += need_y

      self.set_pool(pool_id, pool)

      self.internal_mint_liquidity(user, liquidity)
      
      return lpt_id

   # Append liquidity to the specified lpt
   # @param lpt_id: a string like pool_id|inner_id
   # @param amount_x: the number of token X users expect to add liquidity to use
   # @param amount_y: the number of token Y users expect to add liquidity to use
   # @param min_amount_x: the minimum number of token X users expect to add liquidity to use
   # @param min_amount_y: the minimum number of token Y users expect to add liquidity to use
   def append_liquidity(self, user_id, lpt_id, amount_x, amount_y, min_amount_x, min_amount_y ):
      user = self.get_user(user_id)
      liquidity = self.get_user_liquidity(lpt_id)
      if liquidity is None:
         print("No liquidity found, lpt_id :", lpt_id)
         return (0, 0)
      
      if (user_id != liquidity.owner_id):
         print("E215_NOT_LIQUIDITY_OWNER : "+self.name)
         raise Exception("user_id != liquidity.owner_id")
         return (0,0)
      
      pool = self.get_pool(liquidity.pool_id)

      (new_liquidity, need_x, need_y, acc_fee_x_in_128, acc_fee_y_in_128) = pool.internal_add_liquidity(liquidity.left_point, liquidity.right_point, amount_x, amount_y, min_amount_x, min_amount_y)

      liquidity.get_unclaimed_fee(acc_fee_x_in_128, acc_fee_y_in_128)
      new_fee_x = liquidity.unclaimed_fee_x
      new_fee_y = liquidity.unclaimed_fee_y

      pool.total_liquidity += new_liquidity
      pool.total_x += need_x
      pool.total_y += need_y
      pool.total_x -= new_fee_x
      pool.total_y -= new_fee_y

      # refund
      refund_x = amount_x - need_x + new_fee_x
      refund_y = amount_y - need_y + new_fee_y

      # update lpt
      liquidity.amount += new_liquidity
      liquidity.last_fee_scale_x_128 = acc_fee_x_in_128
      liquidity.last_fee_scale_y_128 = acc_fee_y_in_128
      self.set_user(user.user_id, user)
      self.set_pool(liquidity.pool_id, pool) 

      self.set_user_liquidity(lpt_id, liquidity)
      pass

   # Users can merge lpts with the same left and right boundaries in the same pool
   # @param lpt_id: a string like pool_id|inner_id
   # @param lpt_id_list
   def merge_liquidity(self, user_id, lpt_id, lpt_id_list):
      if len(lpt_id_list) == 0:
         print("E216_INVALID_LPT_LIST")
         return
      retain_liquidity = self.get_user_liquidity(lpt_id)
      
      if retain_liquidity.owner_id != user_id:
         print("E215_NOT_LIQUIDITY_OWNER : "+self.name)
         raise Exception("user_id != liquidity.owner_id")
         return

      pool = self.get_pool(retain_liquidity.pool_id) 

      remove_token_x = 0;
      remove_token_y = 0;
      remove_fee_x = 0;
      remove_fee_y = 0;

      merge_lpt_ids = ""
      
      for item in lpt_id_list:
         if merge_lpt_ids == "":
            merge_lpt_ids = merge_lpt_ids+""+item
         else:
            merge_lpt_ids = merge_lpt_ids+","+item
         
         user = self.get_user(user_id)
         
         liquidity = self.get_user_liquidity(item)
         
         if item == lpt_id or liquidity.owner_id != retain_liquidity.owner_id or liquidity.pool_id != retain_liquidity.pool_id or\
            liquidity.left_point != retain_liquidity.left_point or liquidity.right_point != retain_liquidity.right_point:
            print("E216_INVALID_LPT_LIST : "+self.name)
            raise Exception("E216_INVALID_LPT_LIST")
            return
            

         (remove_x, remove_y, acc_fee_x_in_128, acc_fee_y_in_128) = pool.internal_remove_liquidity(liquidity.amount, liquidity.left_point, liquidity.right_point, 0, 0);
         
         liquidity.get_unclaimed_fee(acc_fee_x_in_128, acc_fee_y_in_128)
         fee_x = liquidity.unclaimed_fee_x
         fee_y = liquidity.unclaimed_fee_y

         remove_token_x += remove_x
         remove_token_y += remove_y
         remove_fee_x += fee_x
         remove_fee_y += fee_y

         pool.total_liquidity -= liquidity.amount
         pool.total_x -= remove_x + fee_x
         pool.total_y -= remove_y + fee_y
         self.internal_burn_liquidity(user, liquidity)


      (new_liquidity, need_x, need_y, acc_fee_x_in_128, acc_fee_y_in_128) = pool.internal_add_liquidity(retain_liquidity.left_point, retain_liquidity.right_point, remove_token_x, remove_token_y, 0, 0)
      retain_liquidity.get_unclaimed_fee(acc_fee_x_in_128, acc_fee_y_in_128)
      new_fee_x = retain_liquidity.unclaimed_fee_x
      new_fee_y = retain_liquidity.unclaimed_fee_y

      pool.total_liquidity += new_liquidity
      pool.total_x += need_x
      pool.total_y += need_y
      pool.total_x -= new_fee_x
      pool.total_y -= new_fee_y

      refund_x = remove_token_x - need_x + new_fee_x + remove_fee_x
      refund_y = remove_token_y - need_y + new_fee_y + remove_fee_y


      retain_liquidity.amount += new_liquidity;
      retain_liquidity.last_fee_scale_x_128 = acc_fee_x_in_128
      retain_liquidity.last_fee_scale_y_128 = acc_fee_y_in_128

      self.et_pool(retain_liquidity.pool_id, pool);
      self.set_user_liquidity(lpt_id, retain_liquidity)
   
      pass

   # If all amount in this lp token is removed, 
   # it will be a burn operation else decrease amount of this lp token
   # @param lpt_id: a string like pool_id|inner_id
   # @param amount: amount of liquidity.
   # @param min_amount_x: removing liquidity will at least give you the number of token X
   # @param min_amount_y: removing liquidity will at least give you the number of token Y
   # @return (amount_x, amount_y)
   # amount_x, balance of tokenX released into inner account (including feeX);
   # amount_y, balance of tokenY released into inner account (including feeY);
   # Note: remove_liquidity with 0 amount, 0 min_amount_x, 0 min_amount_y means claim
   def remove_liquidity(self, user_id, lpt_id, amount, min_amount_x, min_amount_y):
      user = self.get_user(user_id)
      liquidity = self.get_user_liquidity(lpt_id)
      if liquidity is None:
         print("E215_NOT_LIQUIDITY")
         return (0,0)
      if (user_id != liquidity.owner_id):
         print("E215_NOT_LIQUIDITY_OWNER : "+self.name)
         raise Exception("user_id != liquidity.owner_id")         
         return (0,0)
      pool = self.get_pool(liquidity.pool_id)

      remove_liquidity = liquidity.amount
      if amount < liquidity.amount:
         remove_liquidity = amount

      # ignore mining part

      (remove_x, remove_y, acc_fee_x_in_128, acc_fee_y_in_128) = pool.internal_remove_liquidity(remove_liquidity, liquidity.left_point, liquidity.right_point, min_amount_x, min_amount_y)
      liquidity.get_unclaimed_fee(acc_fee_x_in_128, acc_fee_y_in_128)

      new_fee_x = liquidity.unclaimed_fee_x
      new_fee_y = liquidity.unclaimed_fee_y

      liquidity.amount -= remove_liquidity

      refund_x = remove_x + new_fee_x
      refund_y = remove_y + new_fee_y


      pool.total_liquidity -= remove_liquidity
      pool.total_x -= refund_x
      pool.total_y -= refund_y

      self.set_pool(liquidity.pool_id, pool) 

      if liquidity.amount > 0:
         liquidity.last_fee_scale_x_128 = acc_fee_x_in_128
         liquidity.last_fee_scale_y_128 = acc_fee_y_in_128
         self.set_user(user.user_id, user);
         self.set_user_liquidity(lpt_id, liquidity)
      else:
         self.internal_burn_liquidity(user, liquidity)


      return (refund_x, refund_y)
      pass


   # @param pool_ids: all pools participating in swap
   # @param input_token: the swap-in token, must be in pool_ids[0].tokens
   # @param output_token: the swap-out token, must be in pool_ids[-1].tokens
   # @param input_amount: the amount of swap-in token
   # @param tag
   # @return estimated output token amount      
   def quote(self, account_id, pool_ids, input_token, output_token, input_amount, tag):
      quote_failed = {"amount": 0, "tag": tag}
      if self.state == PAUSED:
         return quote_failed
      
      pool_record = []            
      protocol_fee_rate = self.protocol_fee_rate
      
      vip_info = self.vip_users.get(account_id,{})
      
      next_input_token_or_last_output_token = input_token
      next_input_amount_or_actual_output = input_amount

      for pool_id in pool_ids:
         pool = self.get_pool(pool_id)
         if pool is None:
            print("quote: quote_failed1")
            return quote_failed
         
         if pool.state == PAUSED:
            print("quote: quote_failed2")
            return quote_failed
         if pool.token_x+"|"+pool.token_y in pool_record:
            print("quote: quote_failed3")
            return quote_failed
         pool_record.append(pool.token_x+"|"+pool.token_y)

         pool_fee = pool.get_pool_fee_by_user(vip_info)
         
         is_finished = None
         if next_input_token_or_last_output_token == pool.token_x:
            (_, out_amount, is_finished, _, _) = pool.internal_x_swap_y(pool_fee, protocol_fee_rate, next_input_amount_or_actual_output, -799999, True)
            next_input_token_or_last_output_token = pool.token_y
            next_input_amount_or_actual_output = out_amount
         elif next_input_token_or_last_output_token == pool.token_y:
            (_, out_amount, is_finished, _, _) = pool.internal_y_swap_x(pool_fee, protocol_fee_rate, next_input_amount_or_actual_output, 799999, True)
            next_input_token_or_last_output_token = pool.token_x.clone()
            next_input_amount_or_actual_output = out_amount
         else:
            print("quote: quote_failed4")
            return quote_failed
         
         if is_finished == False:
            print("quote: quote_failed5")
            return quote_failed

      if output_token != next_input_token_or_last_output_token:
         print("quote: quote_failed6")
         return quote_failed
      
      return { "amount": next_input_amount_or_actual_output, "tag": tag }

   # @param pool_ids: all pools participating in swap
   # @param input_token: the swap-in token, must be in pool_ids[-1].tokens
   # @param output_token: the swap-out token, must be in pool_ids[0].tokens
   # @param output_amount: the amount of swap-out token
   # @param tag
   # @return estimated input token amount   
   def quote_by_output(self, pool_ids, input_token, output_token, output_amount, tag):
      quote_failed = {"amount": 0, "tag": tag}
      if self.state == PAUSED:
         return quote_failed
      pool_record = []
      protocol_fee_rate = self.protocol_fee_rate
      
      next_desire_token = output_token
      next_desire_amount = output_amount
      for pool_id in pool_ids:
         pool = self.get_pool(pool_id)
         if pool is None:
            print("quote_by_output: quote_failed1")
            return quote_failed
                  
         if pool.state == PAUSED:
            print("quote_by_output: quote_failed2")
            return quote_failed
         if pool.token_x+"|"+pool.token_y in pool_record:
            print("quote_by_output: quote_failed3")
            return quote_failed
         pool_record.append(pool.token_x+"|"+pool.token_y)

         is_finished = None

         if next_desire_token == pool.token_x:
            (need_amount, _, is_finished) = pool.internal_y_swap_x_desire_x(protocol_fee_rate, next_desire_amount, 800001, True)
            next_desire_token = pool.token_y
            next_desire_amount = need_amount
         elif next_desire_token == pool.token_y:
            (need_amount, _, is_finished) = pool.internal_x_swap_y_desire_y(protocol_fee_rate, next_desire_amount, -800001, True)
            next_desire_token = pool.token_x
            next_desire_amount = need_amount
         else:
            print("quote_by_output: quote_failed4")
            return quote_failed
        
         if is_finished == False:
            print("quote_by_output: quote_failed5")
            return quote_failed
      
      
      if input_token != next_desire_token:
         print("quote_by_output: quote_failed6")
         return quote_failed
      
      return { "amount": next_desire_amount, "tag":tag }

   # @param account_id
   # @param pool_ids: all pools participating in swap
   # @param input_token: the swap-in token, must be in pool_ids[0].tokens
   # @param input_amount: the amount of swap-in token
   # @param output_token: the swap-out token, must be in pool_ids[-1].tokens
   # @param min_output_amount: minimum number of swap-out token to be obtained
   # @return actual got output token amount
   # internal_swap(self, account_id: &AccountId, pool_ids: Vec<PoolId>, input_token: &AccountId, input_amount: Balance, output_token: &AccountId, min_output_amount: Balance )
   def swap(self, account_id, pool_ids, input_token: str, input_amount: int, output_token: str, min_output_amount: int ):
      pool_record = []
      fee_tokens = []
      total_fee_amounts = []
      protocol_fee_amounts = []
      protocol_fee_rate = self.protocol_fee_rate
      vip_info = self.vip_users.get(account_id,{})

      next_input_token_or_last_output_token = input_token
      next_input_amount_or_actual_output = input_amount
      

      for pool_id in pool_ids:
         pool = self.get_pool(pool_id)
         if pool is None:
            print("internal_swap: failed1")
            return None
         
         if pool.state == PAUSED:
            print("internal_swap: failed2")
            return None
         if pool.token_x+"|"+pool.token_y in pool_record:
            print("internal_swap: E206_DUPLICATE_POOL")
            return None
         pool_record.append(pool.token_x+"|"+pool.token_y)

         is_finished = None         
         #print("next_input_token_or_last_output_token = ",next_input_token_or_last_output_token)
         #print("pool.token_x = ",pool.token_x,",pool.token_y = ",pool.token_y)
         #print("vip_info",vip_info)
         pool_fee = pool.get_pool_fee_by_user(vip_info)
         
         if next_input_token_or_last_output_token == pool.token_x:
            (actual_cost, out_amount, is_finished, total_fee, protocol_fee) = pool.internal_x_swap_y(pool_fee, protocol_fee_rate, next_input_amount_or_actual_output, -799999, False)

            if is_finished == False:
               print(f"swap: input_amount: {input_amount}")
               print(f"swap: ERR_TOKEN_[{pool.token_y}]_NOT_ENOUGH : "+self.name)
               raise Exception("is_finished == False")
               return None

            pool.total_x += actual_cost
            pool.total_y -= out_amount
            pool.volume_x_in += actual_cost
            pool.volume_y_out += out_amount
            
            fee_tokens.append(pool.token_x)
            total_fee_amounts.append(total_fee)
            protocol_fee_amounts.append(protocol_fee)

            next_input_token_or_last_output_token = pool.token_y
            next_input_amount_or_actual_output = out_amount
            
         elif next_input_token_or_last_output_token == pool.token_y:
            (actual_cost, out_amount, is_finished, total_fee, protocol_fee) = pool.internal_y_swap_x(pool_fee, protocol_fee_rate, next_input_amount_or_actual_output, 799999, False)
            if is_finished == False:
               print(f"swap: input_amount: {input_amount}")
               print(f"swap: ERR_TOKEN_[{pool.token_x}]_NOT_ENOUGH : "+self.name)
               raise Exception(f"swap: ERR_TOKEN_[{pool.token_x}]_NOT_ENOUGH")
               return None

            pool.total_y += actual_cost
            pool.total_x -= out_amount
            pool.volume_y_in += actual_cost
            pool.volume_x_out += out_amount

            fee_tokens.append(pool.token_y)
            total_fee_amounts.append(total_fee)
            protocol_fee_amounts.append(protocol_fee)

            next_input_token_or_last_output_token = pool.token_x
            next_input_amount_or_actual_output = out_amount
         else:
            print("E404_INVALID_POOL_IDS : "+self.name)
            return None

         self.set_pool(pool_id, pool)

      if output_token != next_input_token_or_last_output_token:
         print("E212_INVALID_OUTPUT_TOKEN : "+self.name)
         raise Exception("output_token != next_input_token_or_last_output_token")
         return None

      if next_input_amount_or_actual_output < min_output_amount:
         print("next_input_amount_or_actual_output : "+str(next_input_amount_or_actual_output)+", min_output_amount : "+str(min_output_amount))
         print("E204_SLIPPAGE_ERR : "+self.name)
         raise Exception("next_input_amount_or_actual_output < min_output_amount")
         return None

      return next_input_amount_or_actual_output
   

   # @param account_id
   # @param pool_ids: all pools participating in swap
   # @param input_token: the swap-in token, must be in pool_ids[-1].tokens
   # @param max_input_amount: maximum amount of swap-in token to pay
   # @param output_token: the swap-out token, must be in pool_ids[0].tokens
   # @param output_amount: the amount of swap-out token
   # @return actual used input token amount
   # internal_swap_by_output(self, account_id: &AccountId, pool_ids: Vec<PoolId>, input_token: &AccountId, max_input_amount: Balance, output_token: &AccountId, output_amount: Balance )
   def swap_by_output(self, account_id, pool_ids, input_token: str, max_input_amount: int, output_token: str, output_amount: int ):
      pool_record = []
      fee_tokens = []
      total_fee_amounts = []
      protocol_fee_amounts = []
      protocol_fee_rate = self.protocol_fee_rate
      vip_info = self.vip_users.get(account_id,{})
      
      next_desire_token = output_token
      next_desire_amount = output_amount
      actual_output_amount = output_amount

      for pool_id in pool_ids:
         pool = self.get_pool(pool_id)
         if pool is None:
            print("internal_swap_by_output: None")
            return None
         if pool.state == PAUSED:
            print("internal_swap_by_output: PAUSED")
            return None                  
         if pool.token_x+"|"+pool.token_y in pool_record:
            print("internal_swap_by_output: E206_DUPLICATE_POOL")
            return quote_failed
         pool_record.append(pool.token_x+"|"+pool.token_y)

         is_finished = None      
         pool_fee = pool.get_pool_fee_by_user(vip_info)
         if next_desire_token == pool.token_x:
            (need_amount, acquire_amount, is_finished, total_fee, protocol_fee) = pool.internal_y_swap_x_desire_x(pool_fee, protocol_fee_rate, next_desire_amount, 800001, False)
            if is_finished == False:
               print(f"swap_by_output: ERR_TOKEN_[{pool.token_x}]_NOT_ENOUGH : "+self.name)
               raise Exception(f"swap_by_output: ERR_TOKEN_[{pool.token_x}]_NOT_ENOUGH")
               return None

            pool.total_y += need_amount
            pool.total_x -= acquire_amount
            pool.volume_y_in += need_amount
            pool.volume_x_out += acquire_amount

            fee_tokens.append(pool.token_x)
            total_fee_amounts.append(total_fee)
            protocol_fee_amounts.append(protocol_fee)

            actual_output_amount = acquire_amount
            next_desire_token = pool.token_y
            next_desire_amount = need_amount
         elif next_desire_token == pool.token_y:
            (need_amount, acquire_amount, is_finished, total_fee, protocol_fee) = pool.internal_x_swap_y_desire_y(pool_fee, protocol_fee_rate, next_desire_amount, -800001, False)
            if is_finished == False:
               print(f"swap_by_output: ERR_TOKEN_[{pool.token_y}]_NOT_ENOUGH : "+self.name)
               raise Exception(f"swap_by_output: ERR_TOKEN_[{pool.token_y}]_NOT_ENOUGH")
               return None

            pool.total_x += need_amount
            pool.total_y -= acquire_amount
            pool.volume_x_in += need_amount
            pool.volume_y_out += acquire_amount

            fee_tokens.append(pool.token_y)
            total_fee_amounts.append(total_fee)
            protocol_fee_amounts.append(protocol_fee)

            actual_output_amount = acquire_amount
            next_desire_token = pool.token_x
            next_desire_amount = need_amount
         else:
            print("E404_INVALID_POOL_IDS : "+self.name)
            raise Exception("E404_INVALID_POOL_IDS")
            return None

         self.set_pool(pool_id, pool)

      (next_desire_token, next_desire_amount, actual_output_amount)

      if input_token != next_desire_token:
         print("E213_INVALID_INPUT_TOKEN : "+self.name)
         raise Exception("input_token != next_desire_token")
         return None

      if next_desire_amount > max_input_amount:
         print("E204_SLIPPAGE_ERR : "+self.name)
         raise Exception("next_desire_amount > max_input_amount")
         return None

      return next_desire_amount

   # @param account_id
   # @param pool_id
   # @param input_token: the swap-in token
   # @param input_amount: the amount of swap-in token
   # @param stop_point: low_boundary_point or hight_boundary_point
   # @return actual cost input token amount
   # internal_swap_by_stop_point( &mut self, account_id: &AccountId, pool_id: &PoolId, input_token: &AccountId, input_amount: Balance, stop_point: i32) -> Balance
   def swap_by_stop_point( self, account_id: str, pool_id: str, input_token: str, input_amount: int, stop_point: int):
      pool = self.get_pool(pool_id)
      fee_tokens = []
      total_fee_amounts = []
      protocol_fee_amounts = []
      protocol_fee_rate = self.protocol_fee_rate
      vip_info = self.vip_users.get(account_id,{})
      pool_fee = pool.get_pool_fee_by_user(vip_info)

      output_token = ""
      actual_input_amount = 0
      actual_output_amount = 0

      if input_token == pool.token_x:
         (actual_input_amount, actual_output_amount, _, total_fee, protocol_fee) = pool.internal_x_swap_y(pool_fee, protocol_fee_rate, input_amount, stop_point, False)

         pool.total_x += actual_input_amount
         pool.total_y -= actual_output_amount
         pool.volume_x_in += actual_input_amount
         pool.volume_y_out += actual_output_amount

         fee_tokens.append(pool.token_x)
         total_fee_amounts.append(total_fee)
         protocol_fee_amounts.append(protocol_fee)

         output_token = pool.token_y
      elif input_token == pool.token_y:
         (actual_input_amount, actual_output_amount, _, total_fee, protocol_fee) = pool.internal_y_swap_x(pool_fee, protocol_fee_rate, input_amount, stop_point, False)

         pool.total_y += actual_input_amount
         pool.total_x -= actual_output_amount
         pool.volume_y_in += actual_input_amount
         pool.volume_x_out += actual_output_amount

         fee_tokens.append(pool.token_y)
         total_fee_amounts.append(total_fee)
         protocol_fee_amounts.append(protocol_fee)

         output_token = pool.token_x
      else:
         print("E404_INVALID_POOL_IDS")
         return 0
        
      self.set_pool(pool_id, pool)

      return actual_input_amount


   #pub fn get_liquidity_range(self, pool_id: PoolId,
   #      left_point: i32,  // N * pointDelta, -800000 min
   #      right_point: i32, // N * pointDelta, +800000 max
   #   ) -> HashMap<i32, RangeInfo>
   def get_liquidity_range(self, pool_id: str, left_point: int, right_point: int):
      ret = {}
      if left_point > right_point or left_point < LEFT_MOST_POINT or right_point > RIGHT_MOST_POINT:
         print("E202_ILLEGAL_POINT")
         return ret
      
      pool = self.get_pool(pool_id)

      if left_point >= pool.current_point:
         range_info_to_the_left_of_cp(pool, left_point, right_point, ret)
      elif right_point <= pool.current_point:
         range_info_to_the_right_of_cp(pool, left_point, right_point, ret)
      else:
         range_info_to_the_right_of_cp(pool, left_point, pool.current_point, ret)
         range_info_to_the_left_of_cp(pool, pool.current_point, right_point, ret)

      return ret

   #pub fn get_pointorder_range(self, pool_id: PoolId, left_point: i32, right_point: i32, ) -> HashMap<i32, PointOrderInfo>
   def get_pointorder_range(self, pool_id: str, left_point: int, right_point: int ):
      ret = {}
      if left_point > right_point or left_point < LEFT_MOST_POINT or right_point > RIGHT_MOST_POINT:
         print("E202_ILLEGAL_POINT")
         raise Exception("E202_ILLEGAL_POINT")
         return ret
      pool = self.get_pool(pool_id)
      
      current_point = left_point
      while current_point <= right_point:
         if pool.point_info.has_active_order(current_point, pool.point_delta):
            order = pool.point_info.get_order_data(current_point)
            
            point_order_info = PointOrderInfo()
            point_order_info.point = current_point
            point_order_info.amount_x = order.selling_x
            point_order_info.amount_y = order.selling_y     
            
            ret[current_point] = point_order_info

         current_point = pool.slot_bitmap.get_nearest_right_valued_slot(current_point, pool.point_delta, right_point)
         if current_point is None:
            break

      return ret


   #pub fn get_marketdepth(self, pool_id: PoolId, depth: u8 ) -> MarketDepth
   def get_marketdepth(self, pool_id: str, depth: int ):
      pool = self.get_pool(pool_id)
      left_slot_boundary = max(LEFT_MOST_POINT / pool.point_delta, pool.current_point / pool.point_delta - MARKET_QUERY_SLOT_LIMIT)
      right_slot_boundary = min(RIGHT_MOST_POINT / pool.point_delta, pool.current_point / pool.point_delta + MARKET_QUERY_SLOT_LIMIT)
      liquidities = {}
      orders = {}

      if pool.point_info.has_active_order(pool.current_point, pool.point_delta):
         order_data = pool.point_info.get_order_data(pool.current_point)
         
         point_order_info = PointOrderInfo()
         point_order_info.point = pool.current_point
         point_order_info.amount_x = order_data.selling_x
         point_order_info.amount_y = order_data.selling_y
         
         orders[pool.current_point] = point_order_info


      range_info_count = depth
      order_count = depth
      range_left_point = pool.current_point
      current_point = pool.current_point
      current_liquidity = pool.liquidity
      while range_info_count != 0 or order_count != 0:
         range_right_point = pool.slot_bitmap.get_nearest_right_valued_slot(current_point, pool.point_delta, right_slot_boundary)
         if range_right_point is not None:
            if pool.point_info.is_endpoint(range_right_point, pool.point_delta) and range_info_count != 0:
               range_info = RangeInfo()
               range_info.left_point = range_left_point
               range_info.right_point = range_right_point
               range_info.amount_l = current_liquidity
               
               liquidities[range_left_point] = range_info
               
               range_left_point = range_right_point
               range_info_count -= 1
               liquidity_data = pool.point_info.get_liquidity_data(range_right_point)
               if liquidity_data.liquidity_delta > 0:
                  current_liquidity += liquidity_data.liquidity_delta
               else:
                  current_liquidity -= (-liquidity_data.liquidity_delta)


            if pool.point_info.has_active_order(range_right_point, pool.point_delta) and order_count != 0:
               order_data = pool.point_info.get_order_data(range_right_point)

               point_order_info = PointOrderInfo()
               point_order_info.point = range_right_point
               point_order_info.amount_x = order_data.selling_x
               point_order_info.amount_y = order_data.selling_y
               
               orders[range_right_point] = point_order_info

               order_count -= 1

            current_point = range_right_point
         else:
            break


      range_info_count = depth
      order_count = depth
      range_right_point = pool.current_point
      current_point = pool.current_point
      
      current_liquidity = pool.liquidity
      
      if pool.point_info.is_endpoint(pool.current_point, pool.point_delta):
         liquidity_data = pool.point_info.get_liquidity_data(pool.current_point)
         if liquidity_data.liquidity_delta > 0:
            current_liquidity = pool.liquidity - liquidity_data.liquidity_delta
         else:
            current_liquidity = pool.liquidity + (-liquidity_data.liquidity_delta)
      
      while range_info_count != 0 or order_count != 0:
         range_left_point = pool.slot_bitmap.get_nearest_left_valued_slot(current_point - 1, pool.point_delta, left_slot_boundary)
         if range_left_point is not None:
            if pool.point_info.is_endpoint(range_left_point, pool.point_delta) and range_info_count != 0:
               range_info = RangeInfo()
               range_info.left_point = range_left_point
               range_info.right_point = range_right_point
               range_info.amount_l = current_liquidity
               
               liquidities[range_left_point] = range_info

               range_right_point = range_left_point
               range_info_count -= 1
               liquidity_data = pool.point_info.get_liquidity_data(range_left_point)
               if liquidity_data.liquidity_delta > 0:
                  current_liquidity -= liquidity_data.liquidity_delta
               else:
                  current_liquidity += (-liquidity_data.liquidity_delta)


            if pool.point_info.has_active_order(range_left_point, pool.point_delta) and order_count != 0:
               order_data = pool.point_info.get_order_data(range_left_point)
               
               point_order_info = PointOrderInfo()
               point_order_info.point = range_left_point
               point_order_info.amount_x = order_data.selling_x
               point_order_info.amount_y = order_data.selling_y
               
               orders[range_left_point] = point_order_info
               
               order_count -= 1

            current_point = range_left_point
         else:
            break

      market_depth = MarketDepth()
      market_depth.pool_id = pool_id
      market_depth.current_point = pool.current_point
      market_depth.amount_l = pool.liquidity
      market_depth.amount_l_x = pool.liquidity_x
      market_depth.liquidities = liquidities
      market_depth.orders = orders

   # Swap to given point and place order
   # @param user_id
   # @param token_id: the selling token
   # @param amount: the amount of selling token for this order
   # @param pool_id: pool of this order
   # @param point
   # @param buy_token
   # @return Option<OrderId>
   #     None: swap has consumed all sell token
   # internal_add_order_with_swap(&mut self, client_id: String, user_id: &AccountId, token_id: &AccountId, amount: Balance, pool_id: &PoolId, point: i32, buy_token: &AccountId) -> Option<OrderId>
   def add_order_with_swap(self, client_id, user_id, token_id, amount, pool_id, point, buy_token):
      pool = self.get_pool(pool_id)
      if pool is None:
         print("Invalid pool_id")
         return None
      if point % pool.point_delta != 0:
         print("E202_ILLEGAL_POINT")
         raise Exception("E202_ILLEGAL_POINT")
         return None
      fee_tokens = []
      total_fee_amounts = []
      protocol_fee_amounts = []
      protocol_fee_rate = self.protocol_fee_rate
      
      vip_info = self.vip_users.get(user_id,{})
      pool_fee = pool.get_pool_fee_by_user(vip_info)
      
      
      output_token = ""
      swapped_amount = 0
      swap_earn_amount = 0 
      is_finished = False
      total_fee = 0
      protocol_fee = 0
      
      if token_id == pool.token_x:
         (actual_input_amount, actual_output_amount, is_finished, total_fee, protocol_fee) = pool.internal_x_swap_y(pool_fee, protocol_fee_rate, amount, point, False)
         pool.total_x += actual_input_amount
         pool.total_y -= actual_output_amount
         pool.volume_x_in += actual_input_amount
         pool.volume_y_out += actual_output_amount
         
         fee_tokens.append(pool.token_x)
         total_fee_amounts.append(total_fee)
         protocol_fee_amounts.append(protocol_fee)

         output_token = pool.token_y
         swapped_amount = actual_input_amount
         swap_earn_amount = actual_output_amount 
      elif token_id == pool.token_y:
         (actual_input_amount, actual_output_amount, is_finished, total_fee, protocol_fee) = pool.internal_y_swap_x(pool_fee, protocol_fee_rate, amount, point + 1, False)
         pool.total_y += actual_input_amount
         pool.total_x -= actual_output_amount
         pool.volume_y_in += actual_input_amount
         pool.volume_x_out += actual_output_amount

         fee_tokens.append(pool.token_y)
         total_fee_amounts.append(total_fee)
         protocol_fee_amounts.append(protocol_fee)

         output_token = pool.token_x
         swapped_amount = actual_input_amount
         swap_earn_amount = actual_output_amount 
      else:
         print("add_order_with_swap: E305_INVALID_SELLING_TOKEN_ID")

      self.set_pool(pool_id, pool)

      if is_finished:
         return None;

      return self.add_order( client_id, user_id, token_id, amount, pool_id, point, buy_token, swapped_amount, swap_earn_amount)


   # Place order at given point
   # @param user_id: the owner of this order
   # @param token_id: the selling token
   # @param amount: the amount of selling token for this order
   # @param pool_id: pool of this order
   # @param buy_token: the token this order want to buy
   # @return OrderId
   # internal_add_order(&mut self, client_id: String, user_id: &AccountId, token_id: &AccountId, amount: Balance,  pool_id: &PoolId, point: i32, buy_token: &AccountId, swapped_amount: Balance, swap_earn_amount: Balance ) -> OrderId
   def add_order(self, client_id, user_id, token_id, amount, pool_id, point, buy_token, swapped_amount, swap_earn_amount ):
      pool = self.get_pool(pool_id)
      if pool is None:
         print("Invalid pool_id")
         return None

      if point % pool.point_delta != 0:
         print("E202_ILLEGAL_POINT")
         raise Exception("E202_ILLEGAL_POINT")
         return None

      if amount - swapped_amount <= 0:
         print("E307_INVALID_SELLING_AMOUNT")
         raise Exception("E307_INVALID_SELLING_AMOUNT")
         return None

      # no need to update order_keys
      '''
      point_data = None
      point_order = None
      if str(point) in pool.point_info.data.keys():
         point_data = pool.point_info.data[point]
         point_order = point_data.order_data
      '''
      point_data = pool.point_info.get_point_data_or_default(point)
      prev_active_order = point_data.has_active_order()
      point_order = pool.point_info.get_order_data(point)
      #point_order.dump()


      order_id = self.gen_order_id(pool_id)
      order = UserOrder()
      order.order_id = order_id
      order.owner_id = user_id
      order.pool_id = pool_id
      order.point = point
      order.sell_token = token_id
      order.buy_token = buy_token
      order.original_deposit_amount = amount
      order.swap_earn_amount = swap_earn_amount
      order.original_amount = amount - swapped_amount
      order.created_at = datetime.datetime.now()
      order.last_acc_earn = 0
      order.remain_amount = amount - swapped_amount
      order.cancel_amount = 0
      order.bought_amount = 0
      order.unclaimed_amount = 0

      (token_x, token_y, _) = parse_pool_id(pool_id)
      #print("token_x = ",token_x, ", token_y = ",token_y)
      #print("point =",point,", pool.current_point =",pool.current_point)
      if token_x == token_id:
         if buy_token != token_y:
            print("E303_ILLEGAL_BUY_TOKEN")
            return None
         if point < pool.current_point:
            print("E202_ILLEGAL_POINT2")
            return None
         if point > RIGHT_MOST_POINT:
            print("E202_ILLEGAL_POINT3")
            return None
         order.last_acc_earn = point_order.acc_earn_y
         point_order.selling_x += amount - swapped_amount
         pool.total_x += amount - swapped_amount
         pool.total_order_x += amount - swapped_amount
      else:
         if buy_token != token_x:
            print("E303_ILLEGAL_BUY_TOKEN")
            return None
         if point > pool.current_point:
            print("E202_ILLEGAL_POINT4")
            return None
         if point < LEFT_MOST_POINT:
            print("E202_ILLEGAL_POINT5")
            return None
         order.last_acc_earn = point_order.acc_earn_x
         point_order.selling_y += amount - swapped_amount
         pool.total_y += amount - swapped_amount
         pool.total_order_y += amount - swapped_amount
      point_order.user_order_count += 1

      # update order. No need to update user order keys

      # update pool info
      pool.point_info.set_order_data( point, point_order) 
      #pool.point_info.dump()
      
      #pool.point_info.0.insert(&point, &point_data);
      if False == prev_active_order and False == point_data.has_active_liquidity():
         #pool.slot_bitmap.dump()
         pool.slot_bitmap.set_one(point, pool.point_delta)
         #pool.slot_bitmap.dump()
      self.set_pool(pool_id, pool)
      
      self.user_orders[order_id] = order

      return order_id

   # @param order_id
   # @param amount: max cancel amount of selling token
   # @return (actual removed sell token, bought token till last update)
   # Note: cancel_order with 0 amount means claim
   #pub fn cancel_order(&mut self, order_id: OrderId, amount: Option<U128>) -> (U128, U128)
   def cancel_order(self, user_id, order_id, amount):
      order = self.get_user_order(order_id)
      if order is None:
         print("No order found, order_id :", order_id)
         return (0, 0)

      # no need to handle user history order
      if order.owner_id != user_id:
         print("E300_NOT_ORDER_OWNER")
         return (0, 0)

      print("cancel_order. order_id =",order_id,", amount =", amount,", order.point =",order.point)
      
      pool = self.get_pool(order.pool_id)

      #print(pool.point_info.data.keys())
      
      point_data = None
      point_order = None
      if order.point in pool.point_info.data.keys():
         point_data = pool.point_info.data[order.point]
         point_order = point_data.order_data
         print("order.point in pool.point_info.data.keys()")

      #let mut point_data = pool.point_info.0.get(&order.point).unwrap();
      #let mut point_order: OrderData = point_data.order_data.unwrap();
      #order.dump()
      earned = self.internal_update_point_order( order, point_order)
      #order.dump()
      
      # do cancel
      actual_cancel_amount = order.remain_amount
      if amount>0:
         actual_cancel_amount = min(amount, order.remain_amount)
      
      order.cancel_amount += actual_cancel_amount
      order.remain_amount -= actual_cancel_amount

      # update point_data
      if order.is_earn_y():
         pool.total_x -= actual_cancel_amount
         pool.total_y -= earned
         pool.total_order_x -= actual_cancel_amount
         point_order.selling_x -= actual_cancel_amount
      else:
         pool.total_x -= earned
         pool.total_y -= actual_cancel_amount
         pool.total_order_y -= actual_cancel_amount
         point_order.selling_y -= actual_cancel_amount

      
      point_data.order_data = point_order
      if order.remain_amount == 0:
         point_order.user_order_count -= 1
         if point_order.user_order_count == 0:
            pool.total_order_x -= point_order.selling_x
            pool.total_order_y -= point_order.selling_y
            pool.total_x -= point_order.selling_x
            pool.total_y -= point_order.selling_y
            point_data.order_data = None
         
      #point_order.dump()
      if False == point_data.has_active_liquidity() and False == point_data.has_active_order():
         pool.slot_bitmap.set_zero(order.point, pool.point_delta)
      
      if point_data.has_order() or point_data.has_liquidity():
         pool.point_info.set_point_data(order.point, point_data)
      else:
         pool.point_info.remove(order.point)
      
      self.set_pool(order.pool_id, pool)

      # transfer token to user

      # deactive order if needed
      if order.remain_amount == 0:
         # completed order move to user history

         user = self.get_user(user_id)
         if user is None:
            user = User()

         user.completed_order_count += 1
         self.set_user(user_id, user)
         self.user_orders.pop(order_id, None)
      else:
         self.set_user_order(order_id, order)

      print("cancel_order, order.remain_amount = ",order.remain_amount)

      return (actual_cancel_amount, earned)


   # Sync user order with point order, try to claim as much earned as possible
   # @param ue: user order
   # @param po: point order
   # @return earned amount this time
   def internal_update_point_order(self, ue: UserOrder, po: OrderData):
      sqrt_price_96 = get_sqrt_price(ue.point)

      total_earn = po.earn_x
      total_legacy_earn = po.earn_x_legacy
      acc_legacy_earn = po.acc_earn_x_legacy
      cur_acc_earn = po.acc_earn_x

      if ue.is_earn_y():
         total_earn = po.earn_y
         total_legacy_earn = po.earn_y_legacy
         acc_legacy_earn = po.acc_earn_y_legacy
         cur_acc_earn = po.acc_earn_y

      if ue.last_acc_earn < acc_legacy_earn:
         liquidity = mul_fraction_floor(ue.remain_amount,pow_96(), sqrt_price_96)
         earn = mul_fraction_floor(liquidity, pow_96(), sqrt_price_96)
       
         if ue.is_earn_y():
            liquidity = mul_fraction_floor(ue.remain_amount,sqrt_price_96, pow_96())
            earn = mul_fraction_floor(liquidity, sqrt_price_96, pow_96())

         # update po
         if earn > total_legacy_earn:
            # just protect from some rounding errors
            earn = total_legacy_earn
         
         if ue.is_earn_y():
            if po.earn_y_legacy < earn:
               print("Error: po.earn_y_legacy < earn")
               raise Exception("po.earn_y_legacy < earn")
            po.earn_y_legacy -= earn
         else:
            if po.earn_x_legacy < earn:
               print("Error: po.earn_x_legacy < earn")
               raise Exception("po.earn_x_legacy < earn")
            po.earn_x_legacy -= earn

         # update ue
         ue.last_acc_earn = cur_acc_earn
         ue.remain_amount = 0
         ue.bought_amount += earn
         ue.unclaimed_amount = earn
          
         return earn
      else:
         # this order needs to compete earn
         earn = min(cur_acc_earn - ue.last_acc_earn, total_earn)
         
         liquidity = mul_fraction_ceil(earn, sqrt_price_96, pow_96() )
         sold = mul_fraction_ceil(liquidity, sqrt_price_96, pow_96() )
       
         if ue.is_earn_y():
            liquidity = mul_fraction_ceil(earn, pow_96(), sqrt_price_96)
            sold = mul_fraction_ceil(liquidity, pow_96(), sqrt_price_96)
         '''
         liquidity = mul_fraction_floor(earn, sqrt_price_96, pow_96() )
         sold = mul_fraction_floor(liquidity, sqrt_price_96, pow_96() )
       
         if ue.is_earn_y():
            liquidity = mul_fraction_floor(earn, pow_96(), sqrt_price_96)
            sold = mul_fraction_floor(liquidity, pow_96(), sqrt_price_96)
         '''

         # actual sold should less or equal to remaining, adjust sold and earn if needed
         if sold > ue.remain_amount:
            sold = ue.remain_amount
            liquidity = mul_fraction_floor(sold, pow_96(), sqrt_price_96)
            earn = mul_fraction_floor(liquidity, pow_96(), sqrt_price_96)
            if ue.is_earn_y():
               liquidity = mul_fraction_floor(sold, sqrt_price_96, pow_96())
               earn = mul_fraction_floor(liquidity, sqrt_price_96, pow_96())
         
         # update po
         if earn > total_earn:
            # just protect from some rounding errors
            earn = total_earn
         
         if ue.is_earn_y():
            if po.earn_y < earn:
               print("Error: po.earn_y < earn")
               raise Exception("Error: po.earn_y < earn")
            po.earn_y -= earn
         else:
            if po.earn_x < earn:
               print("Error: po.earn_x < earn")
               raise Exception("po.earn_x < earn")
            po.earn_x -= earn

         # update ue
         ue.last_acc_earn = cur_acc_earn
         ue.remain_amount -= sold
         ue.bought_amount += earn
         ue.unclaimed_amount = earn

         return earn



   def get_user_asset(self, account_id,  token_id):
      pass

   def list_user_assets(self, account_id, from_index,  limit):
      pass

   def is_contract_running(self):
      ContractStateInfo = rest_client.account_resource(DEFAULT_ACCOUNT.address(), f"{DEFAULT_ACCOUNT.address()}::state::ContractPaused")
      if( ContractStateInfo ):
         print("Contract is paused")
         return False
      else:
         print("Contract is running")
         return True
      

   def is_pool_running(self, token1: str, token2: str, fee: int):
      PoolStateInfo = rest_client.account_resource(DEFAULT_ACCOUNT.address(), f"{DEFAULT_ACCOUNT.address()}::state::PoolPaused<{token1}, {token2}, {DEFAULT_ACCOUNT.address()}::fee_type::Fee{fee}>")
      if( PoolStateInfo ):
         print("pool is paused")
         return False
      else:
         print("pool is running")
         return True
   
   def check_pool_state(self, pool_id: str):
      pool = self.get_pool(pool_id)
      print("\n-----------------------------------check_pool_state-------------------------------------")
      print("pool: %s"%pool_id)
      print("current_point = ",pool.current_point, ", sqrt_price_96 = ",get_sqrt_price(pool.current_point))
      
      total_liquidity = pool.total_liquidity
      total_x = pool.total_x
      total_order_x = pool.total_order_x
      total_y = pool.total_y
      total_order_y = pool.total_order_y
      print("total_liquidity = ",pool.total_liquidity)
      print("total_x = ",pool.total_x)
      print("total_order_x = ",pool.total_order_x)
      print("total_y = ",pool.total_y)
      print("total_order_y = ",pool.total_order_y)

      '''
      print("-------------pointinfo & bitmap--------------")
      endpoint_list = []
      for point, point_data in pool.point_info.data.items():
         endpoint_list.append(point)
      print("endpoint_list =",endpoint_list)
      missing_in_bitmap = []
      for point in endpoint_list:
         if pool.slot_bitmap.get_bit(point, pool.point_delta) == 0:
            missing_in_bitmap.append(point)

      abnormal_in_bitmap = []
      endpoint_from_bitmap = pool.slot_bitmap.get_endpoints(pool.point_delta)
      for point in endpoint_from_bitmap:
         if point not in endpoint_list:
            abnormal_in_bitmap.append( point )
      
      print("missing_in_bitmap: ", missing_in_bitmap)
      print("abnormal_in_bitmap: ", abnormal_in_bitmap)
      #'''

      print("-------------pointinfo--------------")
      total_selling_x = 0
      total_selling_y = 0

      #pool.point_info.dump()
      for point, point_data in pool.point_info.data.items():
         #print("point = ",point,", order_data =",point_data.order_data)
         if point_data and point_data.order_data:
            if point >= pool.current_point:
               total_selling_x += point_data.order_data.selling_x
               #print(point,total_selling_x)
            if point <= pool.current_point:
               total_selling_y += point_data.order_data.selling_y
               #print(point,total_selling_y)
      print("total_selling_x = ",total_selling_x)
      print("total_selling_y = ",total_selling_y)

      print("-------------user liquidity & limit order--------------")
      total_user_amount = 0
      total_user_liquidity_token_x = 0
      total_user_liquidity_token_y = 0
      # handle user liquidities
      for LptId, user_liquidity in self.user_liquidities.items():
         if pool_id == user_liquidity.pool_id:
            total_user_amount += user_liquidity.amount
            ( user_liquidity_token_x, user_liquidity_token_y, pool_liquidity, pool_liquidity_x ) = compute_withdraw_x_y(user_liquidity.amount, user_liquidity.left_point, user_liquidity.right_point, pool.current_point,pool.liquidity,pool.liquidity_x)
            total_user_liquidity_token_x += user_liquidity_token_x
            total_user_liquidity_token_y += user_liquidity_token_y
            pool.liquidity = pool_liquidity
            pool.liquidity_x = pool_liquidity_x
      print("total_user_amount = ",total_user_amount)
      

      # handle user limit orders
      total_user_order_x = 0
      total_user_order_y = 0
      total_user_order_x_earned = 0
      total_user_order_y_earned = 0
      
      for k in list(self.user_orders.keys()):
         if pool_id == self.user_orders[k].pool_id:
            is_earn_y = self.user_orders[k].is_earn_y()
            (remain_amount, earned) = self.cancel_order(self.user_orders[k].owner_id, self.user_orders[k].order_id, 0)
            if is_earn_y:
               total_user_order_x += remain_amount
               total_user_order_y_earned += earned
            else:
               total_user_order_y += remain_amount
               total_user_order_x_earned += earned
                  
      print("total_user_liquidity_token_x = ",total_user_liquidity_token_x)
      print("total_user_limit_order_x = ",total_user_order_x)
      print("total_user_limit_order_x_earned = ",total_user_order_x_earned)
      print("total_user_x(liquidity+limit_order) = ",total_user_liquidity_token_x + total_user_order_x_earned + total_user_order_x)
      
      print("total_user_liquidity_token_y = ",total_user_liquidity_token_y)
      print("total_user_limit_order_y = ",total_user_order_y)
      print("total_user_limit_order_y_earned = ",total_user_order_y_earned)
      print("total_user_y(liquidity+limit_order) = ",total_user_liquidity_token_y + total_user_order_y_earned + total_user_order_y)

      if (total_user_liquidity_token_x + total_user_order_x_earned + total_user_order_x) > total_x:
         print(total_x)
         print(total_user_liquidity_token_x + total_user_order_x_earned + total_user_order_x)
         raise Exception("total_x error")
      else:
         print("total_x left =", total_x-(total_user_liquidity_token_x + total_user_order_x_earned + total_user_order_x))
         print("total_order_x left =", total_order_x-total_user_order_x)
      
      if (total_user_liquidity_token_y + total_user_order_y_earned + total_user_order_y) > total_y:
         print(total_y)
         print(total_user_liquidity_token_y + total_user_order_y_earned + total_user_order_y)
         raise Exception("total_y error")
      else:
         print("total_y left =", total_y - (total_user_liquidity_token_y + total_user_order_y_earned + total_user_order_y))
         print("total_order_y left =", total_order_y-total_user_order_y)
      
      if total_selling_x!= total_user_order_x:
         print(total_selling_x)
         print(total_user_order_x)
         raise Exception("total_selling_x!= total_user_order_x")

      if total_selling_y!= total_user_order_y:
         print(total_selling_y)
         print(total_user_order_y)
         raise Exception("total_selling_y!= total_user_order_y")

      print("----------------------------check_pool_state completed----------------------------------\n")

   def dump_pool(self, pool_id):
      pool = self.get_pool(pool_id)
      pool.dump()



def Replay_tx(start_block_height: int, end_block_height: int):
   import requests,json
   dcl = Dcl( protocol_fee_rate = 2000, name = "dcl" )
   dcl.load_dcl_state()
   
   # 1. fetch tx from outside
   # https://mainnet-indexer.ref-finance.com/get-dcl-pool-log?start_block_id=90891178&end_block_id=90894908
   url = 'https://mainnet-indexer.ref-finance.com/get-dcl-pool-log?start_block_id='+str(start_block_height)+'&end_block_id='+str(end_block_height)
   r = requests.get(url)
   json_obj = json.loads(r.text)
   
   # 2. replay
   tx_list = []
   cnt = 0
   for item in json_obj:
      cnt+=1
      if item['tx'] in tx_list:
         continue
      pool_id = ""
      if "pool_id" in item:
         pool_id = item['pool_id']
      elif 'order_id' in item:
         pool_id = item['order_id'].split('#')[0]
      elif 'lpt_id' in item:
         pool_id = item['lpt_id'].split('#')[0]
      elif 'msg' in item:
         msg = json.loads(item['msg'].replace("\\\"", "\""))
         if 'LimitOrderWithSwap' in msg:
            pool_id = msg['LimitOrderWithSwap']['pool_id']
         elif 'LimitOrder' in msg:
            pool_id = msg['LimitOrder']['pool_id']
         elif 'Swap' in msg:
            pool_id = msg['Swap']['pool_ids'][0]
      else:
         #print(item)
         pass
      

      if item['event_method'] == 'liquidity_added':
         #add_liquidity(self, user_id, pool_id: str, left_point: int, right_point: int, amount_x: int, amount_y: int, min_amount_x: int, min_amount_y: int )
         dcl.add_liquidity(item['operator'], pool_id, int(item['left_point']), int(item['right_point']), int(item['amount_x']), int(item['amount_y']), int(item['min_amount_x']), int(item['min_amount_y']) )

      if item['event_method'] == 'liquidity_append':
         #append_liquidity(self, user_id, lpt_id, amount_x, amount_y, min_amount_x, min_amount_y )
         dcl.append_liquidity(item['operator'], item['lpt_id'], int(item['amount_x']), int(item['amount_y']), int(item['min_amount_x']), int(item['min_amount_y']) )

      if item['event_method'] == 'liquidity_removed':
         #remove_liquidity(self, user_id, lpt_id, amount, min_amount_x, min_amount_y)
         #dcl.remove_liquidity(item['operator'], item['lpt_id'], int(item['amount']), int(item['min_amount_x']), int(item['min_amount_y']))
         dcl.remove_liquidity(item['operator'], item['lpt_id'], int(item['amount']), 0, 0)

      if item['event_method'] == 'order_added':
         msg = json.loads(item['msg'].replace("\\\"", "\""))
         if 'LimitOrderWithSwap' in msg:
            #add_order_with_swap(self, client_id, user_id, token_id, amount, pool_id, point, buy_token)
            dcl.add_order_with_swap("", item['operator'],item['token_contract'], int(item['amount']), pool_id, msg['LimitOrderWithSwap']['point'],msg['LimitOrderWithSwap']['buy_token'])
         elif 'LimitOrder' in msg:
            #add_order(self, client_id, user_id, token_id, amount, pool_id, point, buy_token, swapped_amount, swap_earn_amount )
            dcl.add_order("", item['operator'],item['token_id'], int(item['amount']), pool_id, item['point'],item['buy_token'],int(item['swapped_amount']), int(item['swap_earn_amount']))
            
      if item['event_method'] == 'order_cancelled': # cancel_order include order_cancelled & order_completed event
         #cancel_order(self, user_id, order_id, amount)
         if item['amount'] == 'None':
            dcl.cancel_order(item['operator'], item['order_id'], 0)
         else:
            dcl.cancel_order(item['operator'], item['order_id'], int(item['amount']))

      if item['event_method'] == 'order_completed': # means 
         continue
         
      if item['event_method'] == 'swap':
         msg = json.loads(item['msg'].replace("\\\"", "\""))
         if 'LimitOrderWithSwap' in msg: # LimitOrderWithSwap will generate swap event. just skip
            #add_order_with_swap(self, client_id, user_id, token_id, amount, pool_id, point, buy_token)
            dcl.add_order_with_swap("", item['operator'],item['token_contract'], int(item['amount']), pool_id, msg['LimitOrderWithSwap']['point'],msg['LimitOrderWithSwap']['buy_token'])
         else:
            #swap(self, pool_ids, input_token: str, input_amount: int, output_token: str, min_output_amount: int )
            dcl.swap(item['operator'],[pool_id], item['token_contract'], int(item['amount']), msg['Swap']['output_token'], int(msg['Swap']['min_output_amount']))
            #dcl.swap(item['operator'],[pool_id], item['token_contract'], int(item['amount']), msg['Swap']['output_token'], 0)

      tx_list.append(item['tx'])

   
   # 3. save the result to file
   dcl.dump_pools_stats_data()


def generate_endpoint_stats():
   fetch_dcl_files_from_s3(Cfg.LAST_BLOCK_ID)
   Replay_tx(Cfg.LAST_BLOCK_ID+1, Cfg.BLOCK_ID)


if __name__ == "__main__":
   from utils import get_last_block_height
   # replay
   (block_height1, block_height2) = get_last_two_block_height_from_all_s3_folders_list()
   print(block_height1, block_height2)
   #fetch_dcl_files_from_s3(91044865)
   fetch_dcl_files_from_s3(block_height2)
   
   last_block = get_last_block_height()
   last_block_height = last_block['chunks'][0]['height_included']
   if last_block_height is not None:
      Cfg.BLOCK_ID = int(last_block_height)   
   
   Replay_tx(block_height2+1, Cfg.BLOCK_ID)
   #Replay_tx(90891178, 90894908)  
