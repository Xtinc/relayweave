#include "message.h"
#include <algorithm>
#include <iostream>
void check(bool value)
{
    if (!value)
    {
        throw std::runtime_error("binary node frame assertion failed");
    }
}
int main()
{
    try
    {
        LnkFrameHeader f{LnkFrType::Data, true, 4096, 0x0102030405060708, 0x1112131415161718};
        const LnkFrameHeader::Buffer expected{0x4e, 1,    1,    1,    0,    0,    0x10, 0,    1, 2, 3, 4, 5, 6, 7, 8,
                                              0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0, 0, 0, 0, 0, 0, 0, 0};
        check(f.encode() == expected);
        auto parsed = LnkFrameHeader::decode(expected);
        check(parsed.epoch == f.epoch && parsed.id == f.id && parsed.reverse && parsed.body_length == 4096);
        for (auto kind : {LnkFrType::Data, LnkFrType::Fin, LnkFrType::Reset, LnkFrType::Ping, LnkFrType::Pong,
                          LnkFrType::Attach, LnkFrType::Attached})
        {
            f.kind = kind;
            f.reverse = false;
            f.id = kind <= LnkFrType::Reset ? 9 : 0;
            f.sequence = kind == LnkFrType::Ping || kind == LnkFrType::Pong ? 0xffffffffffffffffULL : 0;
            f.body_length = kind == LnkFrType::Fin || kind == LnkFrType::Ping || kind == LnkFrType::Pong ? 0 : 512;
            parsed = LnkFrameHeader::decode(f.encode());
            check(parsed.kind == kind && parsed.id == f.id && parsed.sequence == f.sequence &&
                  parsed.body_length == f.body_length);
        }
        // Malformed headers must fail before the caller allocates or reads a body.
        for (int invalid = 0; invalid < 11; ++invalid)
        {
            bool rejected = false;
            auto bytes = expected;
            if (invalid == 0)
            {
                bytes[0] = 0;
            }
            if (invalid == 1)
            {
                bytes[1] = 2;
            }
            if (invalid == 2)
            {
                bytes[2] = 255;
            }
            if (invalid == 3)
            {
                bytes[3] = 2;
            }
            if (invalid == 4)
            {
                bytes[4] = 0xff;
            }
            if (invalid == 5)
            {
                std::fill(bytes.begin() + 8, bytes.begin() + 16, 0);
            }
            if (invalid == 6)
            {
                std::fill(bytes.begin() + 16, bytes.begin() + 24, 0);
            }
            if (invalid == 7)
            {
                bytes[31] = 1;
            }
            if (invalid == 8)
            {
                bytes[2] = static_cast<std::uint8_t>(LnkFrType::Fin);
            }
            if (invalid == 9)
            {
                bytes[2] = static_cast<std::uint8_t>(LnkFrType::Reset);
            }
            if (invalid == 10)
            {
                bytes[2] = 6; // Removed diagnostic type is not accepted as a business frame.
            }
            try
            {
                static_cast<void>(LnkFrameHeader::decode(bytes));
            }
            catch (const std::exception &)
            {
                rejected = true;
            }
            check(rejected);
        }
        for (const auto bad :
             {LnkFrameHeader{LnkFrType::Ping, false, 0, 7}, LnkFrameHeader{LnkFrType::Pong, true, 0, 7, 0, 1},
              LnkFrameHeader{LnkFrType::Attach, false, 0, 7}, LnkFrameHeader{LnkFrType::Reset, false, 513, 7, 9}})
        {
            bool rejected = false;
            try
            {
                static_cast<void>(LnkFrameHeader::decode(bad.encode()));
            }
            catch (const std::invalid_argument &)
            {
                rejected = true;
            }
            check(rejected);
        }
        static_assert(noexcept(FlowFrame{}.validate()));
        for (const auto &frame : {FlowFrame{7, 9, false, LnkFrType::Data, {}, {}},
                                  FlowFrame{7, 9, true, LnkFrType::Data, BytesBuf(4096), {}},
                                  FlowFrame{7, 9, false, LnkFrType::Fin, {}, {}},
                                  FlowFrame{7, 9, true, LnkFrType::Reset, {}, std::string(512, 'x')}})
        {
            check(frame.validate());
        }
        for (int invalid = 0; invalid < 8; ++invalid)
        {
            FlowFrame frame{7, 9, false, LnkFrType::Data, {0, 255, 3}, {}};
            if (invalid == 0)
            {
                frame.payload.resize(4097);
            }
            if (invalid == 1)
            {
                frame.reason = "unexpected DATA reason";
            }
            if (invalid == 2)
            {
                frame.kind = LnkFrType::Fin;
            }
            if (invalid == 3)
            {
                frame.kind = LnkFrType::Ping;
            }
            if (invalid == 4)
            {
                frame.flow_id = 0;
            }
            if (invalid == 5)
            {
                frame.epoch = 0;
            }
            if (invalid == 6)
            {
                frame.kind = LnkFrType::Reset;
                frame.payload.clear();
                frame.reason.resize(513);
            }
            if (invalid == 7)
            {
                frame.kind = LnkFrType::Fin;
                frame.payload.clear();
                frame.reason = "unexpected FIN reason";
            }
            check(!frame.validate());
        }
        std::cout << "[PASS] binary Node frame headers\n";
    }
    catch (const std::exception &e)
    {
        std::cerr << "[FAIL] " << e.what() << '\n';
        return 1;
    }
}
