package com.yanjing.foruse

import com.izhaowo.cores.utils.JavaHBaseUtils
import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * 数据验证01辅助类。
  */
object DataValidation01Util {

  // 原始系数数组
  //  val coefficientArr = Array(0.05013d, 0.135d, 0.185265d, 0.11718d, 0.06183d, 0.02322d, 0.135d, 0.00711d, 0.10d, 0.185265d)
  val coefficientArr = Array(0.05d, 0.10d, 0.025d, 0.11d, 0.06d, 0.125d, 0.135d, 0.27d, (0.1d / 156d), 0.025d)


  // case class : 合法策划师。
  case class LegalPlanner(worker_id: String, case_dot: Double, reorder_rate: Double, communication_level: Double,
                          design_sense: Double, case_rate: Double, all_score_final: Double, number: Double,
                          text_rating_rate: Double, display_amount: Double, to_store_rate: Double)

  // 1.过滤数据不符合区间的策划师数据。【判断值是否在区间】
  // e
  def legalPlannerFilter(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= 0 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // fi
  def legalPlannerFilter2(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.85d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // fo
  def legalPlannerFilter3(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0.4d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // n
  def legalPlannerFilter4(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1400 && lp.display_amount <= 1600) { // 9.number=0
                      if (lp.to_store_rate >= -0.1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // o
  def legalPlannerFilter5(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= -0.2d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // s
  def legalPlannerFilter6(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= -0.5d) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // t
  def legalPlannerFilter7(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.92d && lp.reorder_rate <= -0.5d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // m4r2-o(ex)
  def legalPlannerFilter8(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // m4r2-t(ex)
  def legalPlannerFilter9(lp: LegalPlanner): LegalPlanner = {
    //        var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.95d && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofi1
  def legalPlannerFilter33(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.85d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofi1’
  def legalPlannerFilter34(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.85d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofi2
  def legalPlannerFilter35(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.8d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofi3
  def legalPlannerFilter36(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.8d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.96d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofo
  def legalPlannerFilter39(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofo’
  def legalPlannerFilter40(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-of2
  def legalPlannerFilter41(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.9d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0.25d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-of3
  def legalPlannerFilter42(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0.25d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot1
  def legalPlannerFilter43(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -0.96d && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot1’
  def legalPlannerFilter44(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -0.96d && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot2
  def legalPlannerFilter53(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.8d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot3
  def legalPlannerFilter54(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.88d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -0.92 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }


  // 望江宾馆模型m4r2-oe
  def legalPlannerFilter55(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.2d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-oe2
  def legalPlannerFilter56(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 0) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-oe3
  def legalPlannerFilter57(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= 0 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-on
  def legalPlannerFilter58(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1650) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-on2
  def legalPlannerFilter59(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.8d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-osvn
  def legalPlannerFilter60(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -0.6d && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-osvn'
  def legalPlannerFilter61(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -0.6d && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-osvn2
  def legalPlannerFilter62(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= -0.5d) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-osx
  def legalPlannerFilter63(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.6d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-osx2
  def legalPlannerFilter64(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.8d && lp.case_dot <= 0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.6d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-osx3
  def legalPlannerFilter65(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= -0.1d) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.6d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-te1
  def legalPlannerFilter66(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 0) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-te2
  def legalPlannerFilter67(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.9d && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.5d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-te3
  def legalPlannerFilter68(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.9d && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= 0 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-tfo1
  def legalPlannerFilter69(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.95d && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-tfo2
  def legalPlannerFilter70(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.95d && lp.reorder_rate <= -0.75) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0.25d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-tfo3
  def legalPlannerFilter71(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.97d && lp.reorder_rate <= -0.7d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0.25d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-tfo4
  def legalPlannerFilter72(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= 0.5d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-tfv1
  def legalPlannerFilter73(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.95d && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -0.5d && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-tn1
  def legalPlannerFilter74(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.98d && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1100 && lp.display_amount <= 1500) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-tn2
  def legalPlannerFilter75(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.95d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.8d && lp.reorder_rate <= -0.75d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 0) { // 3.number=13
          if (lp.design_sense >= -0.75d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -0.9d && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.75d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 0) { // 7.number=1
                  if (lp.text_rating_rate >= -0.6d && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1400 && lp.display_amount <= 1900) { // 9.number=0
                      if (lp.to_store_rate >= -0.1d && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }


  // 望江宾馆模型m4r2-o1fosx
  def legalPlannerFilter76(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.5d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-o2fosx
  def legalPlannerFilter77(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.2d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofo1sx
  def legalPlannerFilter78(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0.5d && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofo2sx
  def legalPlannerFilter79(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 0.8d) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ofosx
  def legalPlannerFilter80(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -1 && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot1fosx
  def legalPlannerFilter81(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.98d && lp.reorder_rate <= -0.8d) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot2fosx
  def legalPlannerFilter82(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.6d && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot3fosx
  def legalPlannerFilter83(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.75d && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }

  // 望江宾馆模型m4r2-ot4fosx
  def legalPlannerFilter84(lp: LegalPlanner): LegalPlanner = {
    // var bool_res = true
    var bool_res = false
    if (lp.case_dot >= -0.85d && lp.case_dot <= 1) { // 1.number=16
      if (lp.reorder_rate >= -0.8d && lp.reorder_rate <= 1) { // 2.number=16
        if (lp.communication_level >= -1 && lp.communication_level <= 1) { // 3.number=13
          if (lp.design_sense >= 0 && lp.design_sense <= 1) { // 4.number=2
            if (lp.case_rate >= -1 && lp.case_rate <= 1) { // 5.number=1
              if (lp.all_score_final >= 0.8d && lp.all_score_final <= 1) { // 6.number=1
                if (lp.number >= -1 && lp.number <= 1) { // 7.number=1
                  if (lp.text_rating_rate >= -1 && lp.text_rating_rate <= 1) { // 8.number=1
                    if (lp.display_amount >= 1200 && lp.display_amount <= 1700) { // 9.number=0
                      if (lp.to_store_rate >= -1 && lp.to_store_rate <= 1) { // 10.number=0[1]
                        bool_res = true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    if (bool_res == true) {
      lp
    } else {
      null
    }
  }


  // 2.过滤无档期的策划师数据。【判断是否有档期，过滤Set】
  def noSchedulePlannerFilter(plannerSet: Set[LegalPlanner], day: String): Set[LegalPlanner] = {
    var set = Set[LegalPlanner]()
    for (lp <- plannerSet) {
      if (hasSchedule4Planner(lp.worker_id, day: String)) {
        set = set.+(lp)
      }
    }
    set
  }

  // 2.1过滤策划师使用有档期。【具体判断是否有档期，针对单个策划师】
  def hasSchedule4Planner(planner_id: String, day: String): Boolean = {
    val planner_name = getPlannerNameById(planner_id).split(" ")(0)
    if (planner_name != null) {
      val url = s"http://master:7979/FuzzyWorkers?workerName=$planner_name&weddate=$day"
      val end: String = getResponse(url)
      val arr = end.split(",")
      if (arr.length >= 8) {
        if ((arr(7).split(":")(1)).toInt > 0) {
          return true
        }
      }
    }
    // todo 发送http消息，通过调用接口获取到最近的策划师档期。
    return false
  }

  // 3.查询策划师昵称。【根据ID查询策划师name】
  def getPlannerNameById(planner_id: String): String = {
    val name = JavaHBaseUtils.getValue("v2_rp_tb_worker", planner_id, "info", "name")
    if (name != null) {
      name
    } else {
      null
    }
  }

  // 4.get方式直接获取数据
  def getResponse(url: String, header: String = null): String = {
    val httpClient = HttpClients.createDefault() // 创建 client 实例
    val get = new HttpGet(url) // 创建 get 实例

    if (header != null) { // 设置 header
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => get.setHeader(key, json.getString(key)))
    }

    val response = httpClient.execute(get) // 发送请求
    EntityUtils.toString(response.getEntity) // 获取返回结果
  }

  // 4.post方式直接获取数据
  def postResponse(url: String, params: String = null, header: String = null): String = {
    val httpClient = HttpClients.createDefault() // 创建 client 实例
    val post = new HttpPost(url) // 创建 post 实例

    // 设置 header
    if (header != null) {
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => post.setHeader(key, json.getString(key)))
    }

    if (params != null) {
      post.setEntity(new StringEntity(params, "UTF-8"))
    }

    val response = httpClient.execute(post) // 创建 client 实例
    EntityUtils.toString(response.getEntity, "UTF-8") // 获取返回结果
  }

  // 5.统计Fv。 模型2
  //  def getTenDataByStatistical(lastPlannerSet: Set[LegalPlanner]): List[String] = {
  //    var fv1 = 0d
  //    var fv2 = 0d
  //    var fv3 = 0d
  //    var fv4 = 0d
  //    var fv5 = 0d
  //    var fv6 = 0d
  //    var fv7 = 0d
  //    var fv8 = 0d
  //    var fv9 = 0d
  //    var fv10 = 0d
  //    lastPlannerSet.foreach(lp => {
  //      val case_dot = lp.case_dot
  //      fv1 = fv1 + (3.1 - (8.6d * Math.pow(case_dot + 0.5d, 2)))
  //      val reorder_rate = lp.reorder_rate
  //      fv2 = fv2 + Math.pow(5.2, (-2.55 * reorder_rate) - 0.8)
  //      val communication_level = lp.communication_level
  //      fv3 = fv3 + ((1 / (0.15 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(communication_level + 0.33, 2) / (-0.45))) + 0.85)
  //      val design_sense = lp.design_sense
  //      //      fv4 = fv4 + (2 - Math.pow(1.9 * design_sense - 0.05, 2))
  //      fv4 = fv4 + ((1 / (0.4 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(design_sense - 0.04, 2) / (-0.32))) + 1)
  //      val case_rate = lp.case_rate
  //      //      fv5 = fv5 + (1 / (1.21 * case_rate + 1.25))
  //      fv5 = fv5 + (Math.pow(-0.18, 4.74 + (0.5 + case_rate)))
  //      val all_score_final = lp.all_score_final
  //      //      fv6 = fv6 + (3 / (1 - (all_score_final * 0.9)))
  //      fv6 = fv6 + (Math.pow(3.4, 2.7 * all_score_final))
  //      val number = lp.number
  //      //      fv7 = fv7 + (5 - 5 * number)
  //      fv7 = fv7 + (Math.pow(0.4, 1.6 * (number - 0.6)))
  //      val text_rating_rate = lp.text_rating_rate
  //      //      fv8 = fv8 + (9 - (15 * Math.pow(text_rating_rate, 2)))
  //      fv8 = fv8 + (7.4 - (Math.pow(3 * text_rating_rate - 0.5, 2)))
  //      val display_amount = lp.display_amount
  //      //      fv9 = fv9 + (-0.000025 * (display_amount * display_amount) + (0.07 * display_amount) - 40)
  //      fv9 = fv9 + (9 - (Math.pow(0.0024 * display_amount - 3.3, 2)))
  //      val to_store_rate = lp.to_store_rate
  //      fv10 = fv10 + ((1 / (Math.sqrt(Math.PI * 2) * (0.01))) * (Math.exp((Math.pow(to_store_rate, 2)) / (-0.0002))))
  //    })
  //    //    List(fv1, fv2, fv3, fv4, fv5, fv6, fv7, fv8, fv9, fv10)
  //    List(fv1.formatted("%.4f"), fv2.formatted("%.4f"), fv3.formatted("%.4f"), fv4.formatted("%.4f"), fv5.formatted("%.4f"), fv6.formatted("%.4f"), fv7.formatted("%.4f"), fv8.formatted("%.4f"), fv9.formatted("%.4f"), fv10.formatted("%.4f"))
  //  }

  // 5.1统计Fv。 模型1——————原始数据
  def getTenDataByStatistical(lastPlannerSet: Set[LegalPlanner]): List[String] = {
    var fv1 = 0d
    var fv2 = 0d
    var fv3 = 0d
    var fv4 = 0d
    var fv5 = 0d
    var fv6 = 0d
    var fv7 = 0d
    var fv8 = 0d
    var fv9 = 0d
    var fv10 = 0d
    lastPlannerSet.foreach(lp => {
      val case_dot = lp.case_dot
      fv1 = fv1 + (4 - Math.pow(4 * case_dot + 2, 2))
      val reorder_rate = lp.reorder_rate
      fv2 = fv2 + Math.pow(2.5, (-8.5 * reorder_rate) - 5.5)
      val communication_level = lp.communication_level
      fv3 = fv3 + ((1 / (0.014 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(communication_level + 0.34, 2) / (-0.000392))))
      val design_sense = lp.design_sense
      fv4 = fv4 + (2 - Math.pow(1.9 * design_sense - 0.05, 2))
      val case_rate = lp.case_rate
      fv5 = fv5 + (1 / (1.21 * case_rate + 1.25))
      val all_score_final = lp.all_score_final
      fv6 = fv6 + (3 / (1 - (all_score_final * 0.9)))
      val number = lp.number
      fv7 = fv7 + (5 - 5 * number)
      val text_rating_rate = lp.text_rating_rate
      fv8 = fv8 + (9 - (15 * Math.pow(text_rating_rate, 2)))
      val display_amount = lp.display_amount
      fv9 = fv9 + (-0.000025 * (display_amount * display_amount) + (0.07 * display_amount) - 40)
      val to_store_rate = lp.to_store_rate
      fv10 = fv10 + ((1 / (Math.sqrt(Math.PI * 2) * (0.01))) * (Math.exp((Math.pow(to_store_rate, 2)) / (-0.0002))))
    })
    List(fv1.formatted("%.4f"), fv2.formatted("%.4f"), fv3.formatted("%.4f"), fv4.formatted("%.4f"), fv5.formatted("%.4f"), fv6.formatted("%.4f"), fv7.formatted("%.4f"), fv8.formatted("%.4f"), fv9.formatted("%.4f"), fv10.formatted("%.4f"))
  }

  // 5.2统计Fv。 模型1——————收缩数据
  def getTenDataByStatistical2(lastPlannerSet: Set[LegalPlanner]): List[String] = {
    var fv1 = 0d
    var fv2 = 0d
    var fv3 = 0d
    var fv4 = 0d
    var fv5 = 0d
    var fv6 = 0d
    var fv7 = 0d
    var fv8 = 0d
    var fv9 = 0d
    var fv10 = 0d
    lastPlannerSet.foreach(lp => {
      val case_dot = lp.case_dot
      fv1 = fv1 + (4 - Math.pow(4 * case_dot + 2, 2))
      val reorder_rate = lp.reorder_rate
      fv2 = fv2 + Math.pow(2.5, (-8.5 * reorder_rate) - 5.5)
      val communication_level = lp.communication_level
      fv3 = fv3 + ((1 / (0.014 * Math.sqrt(2 * Math.PI))) * (Math.exp(Math.pow(communication_level + 0.34, 2) / (-0.000392))))
      val design_sense = lp.design_sense
      fv4 = fv4 + (2 - Math.pow(1.9 * design_sense - 0.05, 2))
      val case_rate = lp.case_rate
      fv5 = fv5 + (1 / (1.21 * case_rate + 1.25))
      val all_score_final = lp.all_score_final
      fv6 = fv6 + (3 / (1 - (all_score_final * 0.9)))
      val number = lp.number
      fv7 = fv7 + (5 - 5 * number)
      val text_rating_rate = lp.text_rating_rate
      fv8 = fv8 + (9 - (15 * Math.pow(text_rating_rate, 2)))
      val display_amount = lp.display_amount
      fv9 = fv9 + (-0.000025 * (display_amount * display_amount) + (0.07 * display_amount) - 40)
      val to_store_rate = lp.to_store_rate
      fv10 = fv10 + ((1 / (Math.sqrt(Math.PI * 2) * (0.01))) * (Math.exp((Math.pow(to_store_rate, 2)) / (-0.0002))))
    })
    List(fv1.toString, fv2.toString, fv3.toString, fv4.toString, fv5.toString, fv6.toString, fv7.toString, fv8.toString, fv9.toString, fv10.toString)
  }

  // 5.3统计Fv。 模型1——————积分数据——————先求公式，再积分，然后乘系数，最后累加求和
  def getTenDataByStatistical3(lastPlannerSet: Set[LegalPlanner]): List[String] = {
    var fv1 = 0d
    var fv2 = 0d
    var fv3 = 0d
    var fv4 = 0d
    var fv5 = 0d
    var fv6 = 0d
    var fv7 = 0d
    var fv8 = 0d
    var fv9 = 0d
    var fv10 = 0d
    lastPlannerSet.foreach(lp => {
      val case_dot = lp.case_dot
      fv1 = getTotalDataFromDiffFormula(1, case_dot, fv1)
      val reorder_rate = lp.reorder_rate
      fv2 = getTotalDataFromDiffFormula(2, reorder_rate, fv2)
      val communication_level = lp.communication_level
      fv3 = getTotalDataFromDiffFormula(3, communication_level, fv3)
      val design_sense = lp.design_sense
      fv4 = getTotalDataFromDiffFormula(4, design_sense, fv4)
      val case_rate = lp.case_rate
      fv5 = getTotalDataFromDiffFormula(5, case_rate, fv5)
      val all_score_final = lp.all_score_final
      fv6 = getTotalDataFromDiffFormula(6, all_score_final, fv6)
      val number = lp.number
      fv7 = getTotalDataFromDiffFormula(7, number, fv7)
      val text_rating_rate = lp.text_rating_rate
      fv8 = getTotalDataFromDiffFormula(8, text_rating_rate, fv8)
      val display_amount = lp.display_amount
      fv9 = getTotalDataFromDiffFormula(9, display_amount, fv9)
      val to_store_rate = lp.to_store_rate
      fv10 = getTotalDataFromDiffFormula(10, to_store_rate, fv10)
    })
    List(fv1.toString, fv2.toString, fv3.toString, fv4.toString, fv5.toString, fv6.toString, fv7.toString, fv8.toString, fv9.toString, fv10.toString)
  }

  /**
    * 根据不同类别，进行公式结果计算，到达步骤: 公式、积分、系数、累加
    *
    * @param number        公式类型
    * @param calculateData 计算数据
    * @param originalData  原始数据
    * @return
    */
  def getTotalDataFromDiffFormula(number: Int, calculateData: Double, originalData: Double): Double = {
    var end = 0d
    number match {
      case 1 => { // 针对公式1进行计算
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("1", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(0) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 2 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("2", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(1) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 3 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("3", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(2) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 4 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("4", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(3) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 5 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("5", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(4) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 6 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("6", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(5) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 7 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("7", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(6) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 8 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("8", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(7) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 9 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("9", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(8) * DataValidationUtils.getIntegralResult(integralArr)
      }
      case 10 => {
        val integralArr = DataValidationUtils.getIntegralDataByInterface5001("10", s"[$calculateData]", "0") // 数值作为下限，进行计算积分
        end = coefficientArr(9) * DataValidationUtils.getIntegralResult(integralArr)
      }
    }
    originalData + end
  }

  def main(args: Array[String]): Unit = {
    //    val url = s"http://master:7979/FuzzyWorkers?workerName=啦啦&weddate=20191201"
    //    val end: String = getResponse(url)
    //    println(end)
    //    val arr = end.split(",")
    //    if (arr.length >= 8) {
    //      if ((arr(7).split(":")(1)).toInt > 0) {
    //        println(true)
    //      } else {
    //        println(false)
    //      }
    //    } else {
    //      println(false)
    //    }
  }
}
