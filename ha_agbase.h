#ifdef USE_PRAGMA_INTERFACE
#pragma interface
#endif

#include "handler.h"
#include "my_global.h"
#include "my_base.h"
#include "thr_lock.h"
#include "dirent.h"
#include <string>
#include <sys/stat.h>
#include <sql_string.h>

class Agbase_share : public Handler_share {
	public:
		mysql_mutex_t mutex;
		THR_LOCK lock;
		Agbase_share();
		~Agbase_share()
		{
			thr_lock_delete(&lock);
			mysql_mutex_destroy(&mutex);
		}
};

class ha_agbase : public handler
{
	THR_LOCK_DATA	lock;
	Agbase_share	*share;
	Agbase_share	*get_share();
        DIR             *d_dir;
        uint64          file_index;
        uint64          num_records;
        String          buffer;
        uchar byte_buffer[IO_SIZE];

	public:
		ha_agbase(handlerton *hton, TABLE_SHARE *table_arg);
		~ha_agbase()
		{
		}

		/* Name of the index type */
		const char *index_type(uint inx) { return "HASH"; }

		/* Engine is statement capable */
		ulonglong table_flags() const
		{
			return (HA_BINLOG_STMT_CAPABLE | HA_NO_TRANSACTIONS |
                          HA_NO_AUTO_INCREMENT | HA_BINLOG_ROW_CAPABLE |
                          HA_REC_NOT_IN_SEQ | HA_NO_BLOBS);
		}

		/* Bitmap of flags that indicate how the storage engine implements
		 * indexes.
		 */
		ulong index_flags(uint inx, uint part, bool all_parts) const
		{
			return 0;
		}

		/* Max supported record length */
		uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }

		/* Don't need keys if the engine does not support indexes. */
		uint max_supported_keys() const { return 0; }
		uint max_supported_key_parts() const { return 0; }
		uint max_supported_key_length() const { return 0; }

		virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }

		virtual double read_time(uint, uint, ha_rows rows)
		{
			return (double) rows / 20.0+1;
		}

		int open(const char *name, int mode, uint test_if_locked);
		int close(void);
		int write_row(uchar *buf);
		int update_row(const uchar *old_data, uchar *new_data);
		int delete_row(const uchar *buf);

		int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
				enum ha_rkey_function find_flag);

		int index_next(uchar *buf);
		int index_prev(uchar *buf);
		int index_first(uchar *buf);
		int index_last(uchar *buf);

		int rnd_init(bool scan);
		int rnd_end();
		int rnd_next(uchar *buf);
		int rnd_pos(uchar *buf, uchar *pos);
		void position(const uchar *record);
		int info(uint);
		int extra(enum ha_extra_function operation);
		int external_lock(THD *thd, int lock_type);
		int delete_all_rows(void);
		ha_rows records_in_range(uint inx, key_range *min_key,
								key_range *max_key);
		int delete_table(const char *from);
		int create(const char *name, TABLE *form,
					HA_CREATE_INFO *create_info);

                // Utilities
                bool has_gif_extension(char const *name);

		enum_alter_inplace_result
		check_if_supported_inplace_alter(TABLE *altered_table,
										Alter_inplace_info *ha_alter_info);

		THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
									enum thr_lock_type lock_type);

};
