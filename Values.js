var ZooKeeper = require ("zookeeper");
var zk = new ZooKeeper({
  connect: "localhost:2181"
 ,timeout: 2000000
 ,debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN
 ,host_order_deterministic: false,
 data_as_buffer:false
});
//This file create the child nodes and put number in it
zk.connect(function(){
   
    console.log ("connected");
    
    //for loop to create a certin number of node 
    for(var i =1;i<=5;i++)
    {
        
        zk.a_create("/Tasks/",i,ZooKeeper.ZOO_SEQUENCE | ZooKeeper.ZOO_EPHEMERAL,
        function (rc, error, path)  {
            if (rc != 0) {
                console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
            } else {
                console.log ("created zk node %s", path);
                process.nextTick(function () {
                 
                });
            }
        });

    }

    
    //zk.close ();
});














   
 
 


