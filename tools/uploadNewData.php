<?php
  // 时间: 2023-03-29
  // 作者: DJW
  // 功能: 向云端上传数据接口

  // 跨域解决
  header("Content-type:text/json;charset=UTF-8");
  // header("Access-Control-Allow-Origin:*");
  // 连接数据库
  $ip = "127.0.0.1";
  $username = "root";
  $password = "zzZZ4144670..";
  $database = "shandong_db";
  $conn = new mysqli($ip, $username, $password, $database) or die("连接失败!");
  mysqli_query($conn, "set character set 'utf8'");
  mysqli_set_charset($conn, 'utf8');
  // 获取json数据
  $input = file_get_contents("php://input");
  $input_json = json_decode($input);
  
  $id = $input_json->id;
  $times = $input_json->times;
  $table_name = $input_json->table_name;

  // 旧表名对应的数据点位设置(要手动进行修改)
  $old_tables_info = array(
    "table_5218r" => ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"],
    "table_insitu" => ["c23", "c24", "c25", "c26", "c27", "c28", "c29"],
    "table_ygqrd" => ["c22"],
    "table_tyn" => ["c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21"],
    "ais_data_history" => ["mmsi", "lon", "lat", "speed", "course", "heading"],
    "ais_data" => ["mmsi", "shipname", "lon", "lat", "speed", "course", "heading", "status", "callsign", "destination", "distance", "shiptype"],
  );
  // 新表名对应的数据点位设置(要手动进行修改)  注意：需要与上面旧表名对应的数据点位一一对应，否则sql语句会出错
  $new_tables_info = array(
    "xiaoguandao_table_5218r" => ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11", "c12", "c13"],
    "xiaoguandao_table_insitu" => ["c23", "c24", "c25", "c26", "c27", "c28", "c29"],
    "xiaoguandao_table_ygqrd" => ["c22"],
    "xiaoguandao_table_tyn" => ["c14", "c15", "c16", "c17", "c18", "c19", "c20", "c21"],
    "xiaoguandao_ais_history_tbl" => ["mmsi", "lon", "lat", "speed", "course", "heading"],
    "xiaoguandao_ais_tbl" => ["mmsi", "shipname", "lon", "lat", "speed", "course", "heading", "status", "callsign", "destination", "distance", "shiptype"],
  );
  // 旧表的数据对应要上传到的新表(要手动进行修改)
  $old_table_to_new_table = array(
    "table_5218r" => "xiaoguandao_table_5218r",
    "table_insitu" => "xiaoguandao_table_insitu",
    "table_ygqrd" => "xiaoguandao_table_ygqrd",
    "table_tyn" => "xiaoguandao_table_tyn",
    "ais_data_history" => "xiaoguandao_ais_history_tbl",
    "ais_data" => "xiaoguandao_ais_tbl",
  );
  
  // ==================================================请求体数据验证==================================================
  $points = array();
  // 遍历旧表设置的信息，判断请求体的table_name是否存在
  foreach($old_tables_info as $key => $value){
    if($table_name == $key){
      $input_array = get_object_vars($input_json);  // 对象转数组
      $point_list = $old_tables_info[$table_name];

      // 验证设置的旧表数据点数量和请求体携带数据点数是否一致
      $body_points_len = count($input_array) - 3;
      $info_points_len = count($point_list);
      if($body_points_len == $info_points_len){
        // 验证请求体数据点名和设置的旧表数据点名是否一致
        foreach($input_array as $k => $value){
          if($k != "id" && $k != "times" && $k != "table_name" && !in_array($k, $point_list)){
            echo json_encode(array('msg' => $k.' not found'));
            return;
          }
        }
        // 新建数据点键值对
        foreach($old_tables_info[$table_name] as $val){
          $points[$val] = null;
        }
        break;
      }else{
        http_response_code(400);
        echo json_encode(array('msg' => 'Number of points error'));
        return;
      }
    }
  }
  // ==================================================保存上传数据==================================================
  if($points){
    $result = false;
    $new_table_name = $old_table_to_new_table[$table_name];
    // 对象转数组
    $input_array = get_object_vars($input_json);
    // 接收请求体的值
    foreach($points as $key => $value){
      $points[$key] = $input_array[$key];
    }
    // ais实时数据需要特殊判断
    if($table_name == "ais_data"){
      $group_array = array();
      $new_point_keys = $new_tables_info[$new_table_name];
      $old_point_values = array_values($points);
      // 组合sql语句
      for($i = 0; $i < count($new_point_keys); $i++){
        if($new_point_keys[$i] != "mmsi"){
          $key_list = ["lon", "lat", "speed", "course", "heading", "distance"];
          // 判断组合数值和字符串
          if(in_array($new_point_keys[$i], $key_list)){
            $group_array[] = "" .$new_point_keys[$i]. "=" .$old_point_values[$i]. "";
          }else{
            $group_array[] = "" .$new_point_keys[$i]. "='" .$old_point_values[$i]. "'";
          }
        }
      }
      // 插入新的mmsi，若不是新的则忽略
      $sql = "INSERT IGNORE INTO " .$new_table_name. "(mmsi) VALUES('" .$points['mmsi']. "');";
      // var_dump($sql);
      $result = mysqli_query($conn, $sql);
      // 更新ais实时数据
      $sql = "UPDATE ".$new_table_name." SET ". join(',', $group_array) ." WHERE mmsi=" .$points['mmsi']. ";";
      // var_dump($sql);
      $result = mysqli_query($conn, $sql);
    }else{ 
      $new_point_keys = $new_tables_info[$new_table_name];
      $old_point_values = array_values($points);
      $sql = "INSERT INTO ".$new_table_name."(times,". join(',', $new_point_keys) .") VALUES('$times',". join(',', $old_point_values) .");";
      // var_dump($sql);
      $result = mysqli_query($conn, $sql);
    }
    
    if($result){
      // echo json_encode("{'msg':'$table_name success'}");
      echo json_encode(array('msg' => $table_name.' success'));
    }else{
      http_response_code(500);
      echo json_encode(array('msg' => 'server error'));
    }
  }else{
    http_response_code(400);
    echo json_encode(array('msg' => 'table_name error'));
    return;
  }
?>