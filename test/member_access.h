#ifndef RELAYWEAVE_TEST_MEMBER_ACCESS_H
#define RELAYWEAVE_TEST_MEMBER_ACCESS_H

#include <functional>
#include <utility>

namespace test_access
{
// Explicit instantiation permits naming private members. Keep all fixture access
// in test translation units: production classes need no friends or test switches.
template <class Tag, auto Member> struct MemberAccess
{
    friend constexpr auto member_pointer(Tag)
    {
        return Member;
    }
};
} // namespace test_access

#define TEST_MEMBER(Class, Member)                                                                                      \
    namespace test_access                                                                                              \
    {                                                                                                                  \
    struct Class##_##Member##_tag                                                                                       \
    {                                                                                                                  \
        friend constexpr auto member_pointer(Class##_##Member##_tag);                                                  \
    };                                                                                                                 \
    template struct MemberAccess<Class##_##Member##_tag, &Class::Member>;                                                \
    template <class Object, class... Args> decltype(auto) Class##_##Member(Object &&object, Args &&...args)                \
    {                                                                                                                  \
        return std::invoke(member_pointer(Class##_##Member##_tag{}), std::forward<Object>(object),                       \
                           std::forward<Args>(args)...);                                                                 \
    }                                                                                                                  \
    }

#endif
