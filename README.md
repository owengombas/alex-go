# ALEX: An Updatable Adaptive Learned Index (Go implementation)
An unofficial Go implmentation of the "ALEX: An Updatable Adaptive Learned Index" paper (https://arxiv.org/abs/1905.08898).
This implementation should not yet be considered production ready, it has been simplified to make it easier to understand the core concepts of the paper.
It is based on the C++ implementation by the authors of the paper (https://github.com/microsoft/ALEX).

## Features supported
- [x] Insertion
- [x] Lookup
- [ ] Duplicates
- [ ] Deletion
- [ ] Bulk loading

## Be careful with large keys
The model are built using a linear regression model, keys will be potentially squared. Which can overflow the float64 type in Go and break the model.

## Credits
```
@inproceedings{Ding_2020, series={SIGMOD/PODS ’20},
   title={ALEX: An Updatable Adaptive Learned Index},
   url={http://dx.doi.org/10.1145/3318464.3389711},
   DOI={10.1145/3318464.3389711},
   booktitle={Proceedings of the 2020 ACM SIGMOD International Conference on Management of Data},
   publisher={ACM},
   author={Ding, Jialin and Minhas, Umar Farooq and Yu, Jia and Wang, Chi and Do, Jaeyoung and Li, Yinan and Zhang, Hantian and Chandramouli, Badrish and Gehrke, Johannes and Kossmann, Donald and Lomet, David and Kraska, Tim},
   year={2020},
   month=may, collection={SIGMOD/PODS ’20}
}
```
