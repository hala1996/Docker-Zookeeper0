
var ZooKeeper = require ("zookeeper");
var zk = new ZooKeeper({
  connect: "localhost:2181"
 ,timeout: 2000000
 ,debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN
 ,host_order_deterministic: false,
 data_as_buffer:false
});

//connect to the server and getTask 
zk.connect(getTask);

function getTask (err,client){
    console.log("client_id",err,client.client_id),client;
   //check if the Taskes node exists
   zk.a_exists("/Tasks",true,function(rc,err,value){
            // console.log("rc",rc,"err",err,"value",value);
                      
             if(!value) //if not exists create it 
             {
                  zk.a_create("/Tasks","some data", null,
                 function (rc, error, path)  {
                     if (rc != 0) {
                         console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
                     } else {
                         console.log ("created zk node %s", path);
                         process.nextTick(function () {
                          zk.close ();
                         });
                     }
                 });
             
             } else { //if exists get the children then get the first child path 
              console.log("tasks node exist");
                        zk.aw_get_children("/Tasks",function(type, state, path )
                        {
                            console.log("type",type,"state",state,"path" ,path);
                              
                        },function(rc,error,children){
                         console.log("Childrens",children);
                         if(children.length>0)
                         
                         var FirstChild
                         {
                             for(var i =0;i<children.length;i++){
                           FirstChild="/Tasks/"+children[i]; 
                           var OperatTask=children[i];//to Lock it when it has been processed 
                           console.log("first child:",FirstChild);
                           getValue(FirstChild);
                           //LockNode(OperatTask);
                        }
                           
                         }
                         
                         
                        });
   
                     }
   
                             
                         
   });

   //get the first child path and then get the value from it 
function getValue(FirstChild)
{
    console.log("get value function:",FirstChild);
    zk.a_get(FirstChild,true,function(rc,err,stat,value){
      //  console.log("rc : " ,rc, "err : ", err, "stat : ",stat, "value : ", value );
       proccessTasks(parseInt( value));
    });
}

/*function LockNode(OperatTask,value)
{

    zk.a_delete_(OperatTask,-1,function(rc,error,value){
        
        console.log("rc",rc,'err',error,'value',value);
        console.log("node is deleted ");
        
    });
    var v=JSON.stringfy({value:value ,machine:client_id});

    zk.a_create("/onProgress/"+client_id,v,null,
    function (rc, error, path)  {
        if (rc != 0) {
            console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
        } else {
            console.log ("created zk node %s", path);
            process.nextTick(function () {
             //zookeeper.close ();
            });
        }
    });
}*/



//get the value from getValue function and do the summation process
function proccessTasks (value)
{
     
    updateTotal(parseInt (value));

}

function updateTotal(value)
{
    console.log("updateTotal",value);
    //check if Total node exist
    zk.a_exists("/TheTotalNode",false,function(rc,err,result)
    {
      if(!result){
        console.log("Not exist");
         
                zk.a_create("/TheTotalNode",value , null,
                function (rc, error, path)  {
                    if (rc != 0) {
                console.log ("zk node create result: %d, error: '%s', path=%s", rc, error, path);
                    } else {
                        console.log ("created zk node %s", path);
                        process.nextTick(function () {
                        zk.close ();
                        });
        }
    });
      } else // if exists get the Total valu from the Total node and add the sum to it  

      {

      zk.a_get("/TheTotalNode",false,function(rc,err,v2,data){
        console.log("Get total: ",'data',data);
        total=parseInt(data);
        total=parseInt(total)+ parseInt(value)+1000; 
        console.log("total=",parseInt(total));
        
         // Store the total value at the Total node 
         var version=parseInt(v2.version);
         console.log("version",version);
        zk.a_set("/TheTotalNode",total,parseInt(version),function(rc,err,stat){
            //console.log('rc:',rc,'err:',err,'stat:',stat,'Total=',total);
            
        })


        //getTasks(err);
    });
           }
           
});
}
}
