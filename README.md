# cs434-project

Team members: Jeong Jaewoo (20170309), Park Junyoung (20170441)

## How to Run?
```
Master:  sbt "Master/run (# of client)"
Worker:  sbt "Worker/run (Master IP) -I (Input Directory) -O (Output Directory)"
```

## Result
1. 3 Workers each with 3GB data

    ![image](https://user-images.githubusercontent.com/67964247/145233629-bb639e11-cd39-4430-b3ae-2c9f8954b79e.png)
    
    machine 1: 21269492 sorted records, machine 2: 21306256 sorted records, machine 3: 21424252 sorted records  => add up to 64000000 records (no lost data)
    
2. 3 Workers each with 30GB data
   - In progress
    
    

## Environment

```
OS : Ubuntu 20.04.3 LTS
JDK : OpenJDK 11.0.11
tested under sbt

sbt : 1.5.5
scala : 2.13.6
scalatest : 3.2.9 (+scalatestplus 3.2.9.0)
junit : 4.13
```

## Plan 
[Initial-plan](https://github.com/candymate/cs434-project/wiki/Initial-Plan)
[Revised-plan](https://github.com/candymate/cs434-project/wiki/Revised-Plan)

## Progress
### Design
- finished designing 

### Implementation
- connection phase finished
- sampling phase finished
- sorting phase finished
- merging phase finished

- currently implementing 
  (i) shuffling
  (ii) checking
  (iii) exploiting multi-core in sorting phase
    
