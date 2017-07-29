/**
*  Copyright (c) 2017 by Contributors 
 */

#include PS_MPI_VAN_H_
#define PS_MPI_VAN_H_
#include "mpi.h"
#include <stdlib.h>
#include <thread>
#include <string>
#include "ps/internal/van.h"
#if _MSC_VER
#define rand_r(x) rand()
#endif

namespace ps {
/**
 * \brief be smart on freeing recved data
 */

inline void FreeData(void *data, void *hint) {
  if (NULL == hint) {
    delete[] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

/**
 * \brief MPI based implementation
 */
class MPIVan : public Van {
 public:
  MPIVan(int nworker, int nserver) {
    nw = nworker;
    ns = nserver;
  }
  virtual ~MPIVan() { }

 protected:
  void Start() override {
  	// start mpi
  	MPI_Init(NULL, NULL);
  	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  	MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);	
  
    receiver_thread_ = std::unique_ptr<std::thread>(
      new std::thread(&MPIVan::Receiving, this));
  }

  void Stop() override {
    return;
  }

  int Send(const Message& msg) {
    int send_bytes = SendMsg(msg);
    send_bytes += send_bytes;
    return send_bytes;
  }

  int Bind(const Node& node, int max_retry) override {
  	return 0;
  }

  void Connect(const Node& node) override {
  	return;
  }

  int SendMsg(const Message& msg) override {
    int meta_size; char* meta_buf;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int recvRank = idToMPIRank(msg.meta.recver);

    int tag = 0;
    MPI_Send(&meta_size, 1, MPI_INT, recvRank, tag++, MPI_COMM_WORLD);
    MPI_Send(meta_buf, meta_size + 1, MPI_CHAR, recvRank, tag++, MPI_COMM_WORLD);

    if (msg.data.size()) {
      int msgDataSize = msg.data.size();
      MPI_Send(&msgDataSize, 1, MPI_INT, recvRank, tag++, MPI_COMM_WORLD);
      for (const auto& d : data) {
        int dataSize = d.size();
        MPI_Send(&dataSize, 1, MPI_INT, recvRank, tag++, MPI_COMM_WORLD);
        MPI_Send(d.data(), dataSize, MPI_CHAR, recvRank, tag++, MPI_COMM_WORLD);
      }
    } else { //msg.data.size() == 0
      int msgDataSize = 0;
      MPI_Send(&msgDataSize, 1, MPI_INT, recvRank, tag++, MPI_COMM_WORLD);
    }
    return 0;
  }

  int RecvMsg(Message* msg) override {
    msg->data.clear();
    int metaSize;
    char *metaBuff;
    int tag = 0;

    MPI_Status mpi_sta;

    MPI_Recv(&metaSize, 1, MPI_INT, MPI_ANY_SOURCE, tag++, MPI_COMM_WORLD, &mpi_sta);
    int sendRank = mpi_sta.MPI_SOURCE;
    metaBuff = (char*) malloc( (metaSize + 1) * sizeof(char) );
    MPI_Recv(metaBuff, metaSize, MPI_CHAR, sendRank, tag++, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    UnpackMeta(metaBuff, metaSize, &(msg->meta));
    free(metaBuff); // ?

    int msgDataSize;
    MPI_Recv(&msgDataSize, 1, MPI_INT, sendRank, tag++, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (msgDataSize) {
      for (int i = 0; i < msgDataSize; ++i) {
        int dataSize; 
        MPI_Recv(&dataSize, 1, MPI_INT, sendRank, tag++, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char *dataBuf = (char*) malloc( (dataSize + 1) * sizeof(char) );
        MPI_Recv(dataBuf, dataSize, MPI_CHAR, sendRank, tag++, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        SArray<char> data;
        data.reset(dataBuf, dataSize, [](char *metaBuff){});
        msg->data.push_back(data);
        free(dataBuf); // ? 
      }
    }

    return 0;
  }

  void Receiving() {
    while (true) {
      Message msg;
      int recv_bytes = RecvMsg(&msg);

      recv_bytes += recv_bytes;

      if (!msg.meta.control.empty()) {
        auto& ctl = msg.meta.control;
        if (ctrl.cmd == Control::TERMINATE) {
          PS_VLOG(1) << my_node_.ShortDebugString() << " is stopped";
          break;
        } else if (ctrl.cmd == Control::ADD_NODE) {
          // nothing will happen. 
        } else if (ctrl.cmd == Control::BARRIER) {// I'm not sure how to rewrite this command 
          // TODO
        } else if (ctrl.cmd == Control::HEARTBEAT) {
          // nothing will happen. 
        }
      } else {
        CHECK_NE(msg.meta.sender. Meta::kEmpty);
        CHECK_NE(msg.meta.recver, Meta::kEmpty);
        CHECK_NE(msg.meta.customer_id, Meta::kEmpty);
        int id = msg.meta.customer_id;
        auto* obj = Postoffice::Get()->GetCustomer(id, 5);
        CHECK(obj) << "timeout (5 sec) to wait App " << id << " ready";
        obj->Accept(msg);
      }
    }
  }

 private:
  int GetNodeID(const char* buf, size_t size) {
  	return Meta::kEmpty;
  }
// MPIRank : [0, nworkers) : worker
//           [nworker, nworker + nserver) : server
// ID:       odd && > 7 : worker 
//           even && > 7 : server
  int idToMPIRank(int id) {
  	int rank = (id - 8) / 2;
  	if (!(id & 1)) {
  		rank += nw;
  	} 
  }

  std::mutex mu_;
  int mpi_size;
  int my_rank;
  int nw; //nworker
  int ns; //nserver
};
}  // namespace ps

#endif  // PS_MPI_VAN_H_