enum class dpfsnetType : int {
    TCP = 0,
    MAX,
};

constexpr const char* dpfsnetTypeStr[] = {
    "tcp",
};

enum class dpfsipc : int {
    DPFS_IPC_CONNECT = 0,       // connect to system
    DPFS_IPC_DISCONNECT,        // disconnect from system
    DPFS_IPC_FOODTRACE,             // trace a request
    DPFS_IPC_MAX,
};

struct dpfs_cmd {
    dpfsipc cmd;                // command type
    int rc;                     // return code
    int size;              // size of parameters
    char data[];                // parameters

};

/*
|IPC|SIZE|DATA|
*/

using msgCallback = int (*)(const dpfs_cmd* cmd);

class CDpfsIPC {
public:
    CDpfsIPC();

    inline const char* name() const { 
        return "DPFS_IPC"; 
    }

private:
    msgCallback m_exe[(int)dpfsipc::DPFS_IPC_MAX];
};


// CTrackIPC::CTrackIPC() : SvcClient(gTrackSet,SERVICE_TRACK)
// {
//         m_exe[IPC_RULE_LOAD]                    = (MsgPipe::CMDCallBack)&CTrackIPC::LoadRule;
//         m_exe[IPC_RULE_UNLOAD]                  = (MsgPipe::CMDCallBack)&CTrackIPC::UnloadRule;
//         m_exe[IPC_RULE_START]                   = (MsgPipe::CMDCallBack)&CTrackIPC::Start;
//         m_exe[IPC_RULE_RESUME]                  = (MsgPipe::CMDCallBack)&CTrackIPC::Resume;
//         m_exe[IPC_RULE_STARTLOAD]               = (MsgPipe::CMDCallBack)&CTrackIPC::StartLoad;
//         m_exe[IPC_RULE_RESUME_FORCE]    = (MsgPipe::CMDCallBack)&CTrackIPC::ResumeRlForce;
//         m_exe[IPC_RULE_DEL]                             = (MsgPipe::CMDCallBack)&CTrackIPC::Del;
//         m_exe[IPC_RULE_STOP]                    = (MsgPipe::CMDCallBack)&CTrackIPC::Stop;
//         m_exe[IPC_RULE_STOPLOAD]                = (MsgPipe::CMDCallBack)&CTrackIPC::StopLoad;
//         m_exe[IPC_RULE_ADD]                             = (MsgPipe::CMDCallBack)&CTrackIPC::Add;
//         m_exe[IPC_RESET_TRACK]                  = (MsgPipe::CMDCallBack)&CTrackIPC::Reset;
//         m_exe[IPC_RULE_CONF]                    = (MsgPipe::CMDCallBack)&CTrackIPC::RuleConf;
//         m_exe[IPC_RULE_INFO]                    = (MsgPipe::CMDCallBack)&CTrackIPC::RuleInfo;
//         m_exe[IPC_DEBUG_TXN]                    = (MsgPipe::CMDCallBack)&CTrackIPC::DebugTxn;
//         //m_exe[IPC_TRACK_SESS]                 = (MsgPipe::CMDCallBack)&CTrackIPC::SessionInfo;
//         m_exe[IPC_RULE_EXTRACT_GATHER]  = (MsgPipe::CMDCallBack)&CTrackIPC::TabGather;
//         m_exe[IPC_RULE_EXTRACK_TBST]    = (MsgPipe::CMDCallBack)&CTrackIPC::TbSt;
//         m_exe[IPC_GET_PUMPER]                   = (MsgPipe::CMDCallBack)&CTrackIPC::GetPumpers;
//         m_exe[IPC_TRACK_ST_ALL]                 = (MsgPipe::CMDCallBack)&CTrackIPC::GetTbStAll;
//         m_exe[IPC_TRACK_ST_TABLE]               = (MsgPipe::CMDCallBack)&CTrackIPC::GetTbStTab;

// }
