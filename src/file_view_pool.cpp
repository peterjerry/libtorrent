/*

Copyright (c) 2006-2016, Arvid Norberg
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the distribution.
    * Neither the name of the author nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

#include "libtorrent/config.hpp"

#include "libtorrent/assert.hpp"
#include "libtorrent/aux_/file_view_pool.hpp"
#include "libtorrent/error_code.hpp"
#include "libtorrent/file_storage.hpp"
#include "libtorrent/units.hpp"
#include "libtorrent/disk_interface.hpp"
#include "libtorrent/aux_/path.hpp"
#ifdef TORRENT_WINDOWS
#include "libtorrent/aux_/win_util.hpp"
#endif

#include <limits>

namespace libtorrent { namespace aux {

	file_view_pool::file_view_pool(int size) : m_size(size) {}
	file_view_pool::~file_view_pool() = default;

	file_view file_view_pool::open_file(storage_index_t st, std::string const& p
		, file_index_t const file_index, file_storage const& fs
		, std::uint32_t const m)
	{
		// potentially used to hold a reference to a file object that's
		// about to be destructed. If we have such object we assign it to
		// this member to be destructed after we release the std::mutex. On some
		// operating systems (such as OSX) closing a file may take a long
		// time. We don't want to hold the std::mutex for that.
		std::shared_ptr<file_mapping> defer_destruction;

		std::unique_lock<std::mutex> l(m_mutex);

		TORRENT_ASSERT(is_complete(p));
		auto const i = m_files.find(std::make_pair(st, file_index));
		if (i != m_files.end())
		{
			lru_map_entry& e = i->second;
			e.last_use = aux::time_now();

			// make sure the write bit is set if we asked for it
			// it's OK to use a read-write file if we just asked for read. But if
			// we asked for write, the file we serve back must be opened in write
			// mode
			if ((e.mode & open_mode_t::write) == 0 && (m & open_mode_t::write))
			{
				defer_destruction = std::move(e.mapping);
				e.mapping = std::make_shared<file_mapping>(
					file_handle(fs.file_path(file_index, p)
						, static_cast<std::size_t>(fs.file_size(file_index)), m), m
					, static_cast<std::size_t>(fs.file_size(file_index)));
				e.mode = m;
				e.last_use = aux::time_now();
			}
			return e.mapping->view();
		}

		if (int(m_files.size()) >= m_size - 1)
		{
			// the file cache is at its maximum size, close
			// the least recently used file
			remove_oldest(l);
		}

		lru_map_entry e(fs.file_path(file_index, p), m
			, static_cast<std::size_t>(fs.file_size(file_index)));
		auto ret = e.mapping->view();
		m_files.insert(std::make_pair(std::make_pair(st, file_index), std::move(e)));

		return ret;
	}

	namespace {

	std::uint32_t to_file_open_mode(std::uint32_t const mode)
	{
		std::uint32_t ret = 0;
		ret = (mode & open_mode_t::write)
			? file_open_mode::read_write
			: file_open_mode::read_write;

// TODO: file_open_mode::no_cache
//		if (mode & file::no_atime) ret |= file_open_mode::no_atime;
		return ret;
	}

	}

	std::vector<open_file_state> file_view_pool::get_status(storage_index_t const st) const
	{
		std::vector<open_file_state> ret;
		{
			std::unique_lock<std::mutex> l(m_mutex);

			auto const start = m_files.lower_bound(std::make_pair(st, file_index_t(0)));
			auto const end = m_files.upper_bound(std::make_pair(st
				, std::numeric_limits<file_index_t>::max()));

			for (auto i = start; i != end; ++i)
			{
				ret.push_back({i->first.second, to_file_open_mode(i->second.mode)
					, i->second.last_use});
			}
		}
		return ret;
	}

	// TODO: make this not be a linear scan
	std::shared_ptr<file_mapping> file_view_pool::remove_oldest(std::unique_lock<std::mutex>&)
	{
		using value_type = decltype(m_files)::value_type;
		auto const i = std::min_element(m_files.begin(), m_files.end()
			, [] (value_type const& lhs, value_type const& rhs)
				{ return lhs.second.last_use < rhs.second.last_use; });
		if (i == m_files.end()) return {};

		auto mapping = std::move(i->second.mapping);
		m_files.erase(i);
		// closing a file may be long running operation (mac os x)
		// let the caller destruct it once it has released the mutex
		return mapping;
	}

	void file_view_pool::release(storage_index_t const st, file_index_t file_index)
	{
		std::unique_lock<std::mutex> l(m_mutex);

		auto const i = m_files.find(std::make_pair(st, file_index));
		if (i == m_files.end()) return;

		auto mapping = std::move(i->second.mapping);
		m_files.erase(i);

		// closing a file may take a long time (mac os x), so make sure
		// we're not holding the mutex
		l.unlock();
	}

	// closes files belonging to the specified
	// storage, or all if none is specified.
	void file_view_pool::release()
	{
		std::unique_lock<std::mutex> l(m_mutex);
		m_files.clear();
		l.unlock();
	}

	void file_view_pool::release(storage_index_t const st)
	{
		std::vector<std::shared_ptr<file_mapping>> defer_destruction;

		std::unique_lock<std::mutex> l(m_mutex);

		auto const begin = m_files.lower_bound(std::make_pair(st, file_index_t(0)));
		auto const end = m_files.upper_bound(std::make_pair(st
				, std::numeric_limits<file_index_t>::max()));

		for (auto it = begin; it != end; ++it)
			defer_destruction.emplace_back(std::move(it->second.mapping));

		if (begin != end) m_files.erase(begin, end);
		l.unlock();
		// the files are closed here while the lock is not held
	}

	void file_view_pool::resize(int size)
	{
		std::unique_lock<std::mutex> l(m_mutex);

		TORRENT_ASSERT(size > 0);

		if (size == m_size) return;
		m_size = size;
		if (int(m_files.size()) <= m_size) return;

		// close the least recently used files
		while (int(m_files.size()) > m_size)
			remove_oldest(l);
	}
}
}

