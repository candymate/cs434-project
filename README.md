# cs434-project

Team members: Jeong Jaewoo (20170309), Park Junyoung (20170441)

# How to Run?
```
Master:  sbt "Master/run (# of client)"
Worker:  sbt "Worker/run (Master IP) -I (Input Directory) -O (Output Directory) (port number) "
```

# Result
## 1. 3 Workers each with 3GB data (Total 9GB)

   ![image](https://user-images.githubusercontent.com/67964247/145233629-bb639e11-cd39-4430-b3ae-2c9f8954b79e.png)
    
   ```
    machine 1: 31904238 sorted records, machine 2: 31959384 sorted records, machine 3: 32136378 sorted records  => add up to 96000000 records (no lost data)
   ```
    
## 2. 3 Workers each with 9GB data (Total 27GB)
   
   ![image](https://user-images.githubusercontent.com/67964247/145250333-d708e470-3308-431c-859c-544c0eea0973.png)

   ```
    machine 1: 95704746 sorted records, machine 2: 95887137 sorted records, machine 3: 96408117 sorted records  => add up to 288000000 records (no lost data)
   ```

## 2-2. Validation between Workers
- Comparison between last element of first machine and first element of second machine 
    ![result4](https://user-images.githubusercontent.com/67964247/145253233-961e9f3a-df47-4392-aafb-879278b6c007.PNG)
    ?VNF{DdqwH (last element of first machine) < ?VNG/EXWo0 (first element of second machine)
    
- Comparison between last elemnt of second machine and first element of last machine 
    ![result5](https://user-images.githubusercontent.com/67964247/145253490-33d214a5-348f-4524-a3fd-8dc8b5ba0d37.PNG)
    _20E\|B;/" (last element of second machine) < _20GS[Z,-b (first element of last machine)


# Environment

```
OS : Ubuntu 20.04.3 LTS
JDK : OpenJDK 11.0.11
tested under sbt

sbt : 1.5.5
scala : 2.13.6
scalatest : 3.2.9 (+scalatestplus 3.2.9.0)
junit : 4.13
```

# Plan 
[Initial-plan](https://github.com/candymate/cs434-project/wiki/Initial-Plan)
[Revised-plan](https://github.com/candymate/cs434-project/wiki/Revised-Plan)

# Progress
finished implementation 
    
