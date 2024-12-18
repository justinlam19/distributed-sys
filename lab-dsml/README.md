# Distributed Machine Learning with AllReduce Ring on the Iris Dataset

Start the GPU Coordinator and GPU Device servers by
```
cd launch
go run server.go
```

Once it has started, in another shell, run the following to train the model on the Iris dataset (100 epochs)
```
cd iris
go run train.go
```