#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#include "my_config.h"
#include "ha_agbase.h"
#include <mysql/plugin.h>
#include "sql_class.h"
#include <iostream>
#include <memory>
#include <my_global.h>
#include <my_dbug.h>

#include <my_decimal.h>
#include <sql_udf.h>
#include <item_row.h>
#include <item_sum.h>
#include <item_cmpfunc.h>

COMPARISON_TYPE get_func_type(Item_func::Functype const);
void create_condition_queue(const Item *item, void *args);

struct cond_tree_ctx
{
  bool ret;
  GifFileType *file;
  AGBASE_CONDITION *cond;
};

static handler *agbase_create_handler(handlerton *hton,
                                      TABLE_SHARE *table,
                                      MEM_ROOT *mem_root);

handlerton *agbase_hton;
mysql_mutex_t agbase_mutex;

static MYSQL_THDVAR_ULONG(varopt_default, PLUGIN_VAR_RQCMDARG,
		"default value of the VAROPT table function", NULL, NULL,
		5, 0, 100, 0);

struct ha_table_option_struct
{
	const char *strparam;
	ulonglong ullparam;
	uint enumparam;
	bool boolparam;
	ulonglong varparam;
};

struct ha_field_option_struct
{
	const char *complex_param;
};

ha_create_table_option agbase_table_option_list[]=
{
	HA_TOPTION_NUMBER("ULL", ullparam, UINT_MAX32, 0, UINT_MAX32, 10),
	HA_TOPTION_STRING("STR", strparam),
	HA_TOPTION_ENUM("one_or_two", enumparam, "one,two", 0),
	HA_TOPTION_BOOL("YESNO", boolparam, 1),
	HA_TOPTION_SYSVAR("VAROPT", varparam, varopt_default),
	HA_TOPTION_END
};

ha_create_table_option agbase_field_option_list[]=
{
	HA_FOPTION_STRING("COMPLEX", complex_param),
	HA_FOPTION_END
};

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ag_key_mutex_agbase, ag_key_mutex_Agbase_share_mutex;

static PSI_mutex_info all_agbase_mutexes[]=
{
        { &ag_key_mutex_agbase, "agbase", PSI_FLAG_GLOBAL },
	{ &ag_key_mutex_Agbase_share_mutex, "Agbase_share::mutex", 0 }
};

static void init_agbase_psi_keys()
{
	const char *category= "agbase";
	int count;

	count = array_elements(all_agbase_mutexes);
	mysql_mutex_register(category, all_agbase_mutexes, count);
}
#endif

static const char *ha_agbase_exts[] = {
	NullS
};

Agbase_share::Agbase_share()
{
	thr_lock_init(&lock);
	mysql_mutex_init(ag_key_mutex_Agbase_share_mutex,
					&mutex, MY_MUTEX_INIT_FAST);
}

static int agbase_init_func(void *p)
{
	DBUG_ENTER("agbase_init_func");

#ifdef HAVE_PSI_INTERFACE
	init_agbase_psi_keys();
#endif

	agbase_hton = (handlerton *)p;
	agbase_hton->state = SHOW_OPTION_YES;
	agbase_hton->create = agbase_create_handler;
	agbase_hton->flags = HTON_CAN_RECREATE;
	agbase_hton->table_options = agbase_table_option_list;
	agbase_hton->field_options = agbase_field_option_list;
	agbase_hton->tablefile_extensions = ha_agbase_exts;

        mysql_mutex_init(ag_key_mutex_agbase, &agbase_mutex, MY_MUTEX_INIT_FAST);

	DBUG_RETURN(0);
}

Agbase_share *ha_agbase::get_share()
{
  Agbase_share *tmp_share;

  DBUG_ENTER("ha_agbase::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<Agbase_share*>(get_ha_share_ptr())))
  {
    tmp_share= new Agbase_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}

static handler* agbase_create_handler(handlerton *hton,
                                       TABLE_SHARE *table,
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_agbase(hton, table);
}

ha_agbase::ha_agbase(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{
  cond_check = false;
  buffer.set((char*)byte_buffer, IO_SIZE, &my_charset_bin);
}


/**
  @brief
  Used for opening tables. The name will be the name of the file.
  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).
  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().
  @see
  handler::ha_open() in handler.cc
*/

int ha_agbase::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_agbase::open");

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);
  condition = NULL;
  got_cond = false;

#ifndef DBUG_OFF
  ha_table_option_struct *options= table->s->option_struct;

  DBUG_ASSERT(options);
  DBUG_PRINT("info", ("strparam: '%-.64s'  ullparam: %llu  enumparam: %u  "\
                      "boolparam: %u",
                      (options->strparam ? options->strparam : "<NULL>"),
                      options->ullparam, options->enumparam, options->boolparam));
#endif

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table.
  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.
  For sql_base.cc look at close_data_tables().
  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_agbase::close(void)
{
  DBUG_ENTER("ha_agbase::close");
  DBUG_RETURN(0);
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.
  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode
  See ha_tina.cc for an example of extracting all of the data as strings.
  ha_berekly.cc has an example of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.
  See the note for update_row() on auto_increments and timestamps. This
  case also applies to write_row().
  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.
  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

int ha_agbase::write_row(uchar *buf)
{
  DBUG_ENTER("ha_agbase::write_row");
  /*
    Example of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  DBUG_RETURN(0);
}


/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.
  @details
  Currently new_data will not have an updated auto_increament record, or
  and updated timestamp field. You can do these for example by doing:
  @code
  if (table->next_number_field && record == table->record[0])
    update_auto_increment();
  @endcode
  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.
  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_agbase::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_agbase::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).
  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.
  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.
  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_agbase::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_agbase::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

int ha_agbase::index_read_map(uchar *buf, const uchar *key,
                               key_part_map keypart_map __attribute__((unused)),
                               enum ha_rkey_function find_flag
                               __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_agbase::index_read");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read forward through the index.
*/

int ha_agbase::index_next(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_agbase::index_next");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  Used to read backwards through the index.
*/

int ha_agbase::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_agbase::index_prev");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.
  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.
  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_agbase::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_agbase::index_first");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.
  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.
  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_agbase::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_agbase::index_last");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the example in the introduction at the top of this file to see when
  rnd_init() is called.
  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.
  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_agbase::rnd_init(bool scan)
{
  DBUG_ENTER("ha_agbase::rnd_init");
  d_dir = opendir("/home/ag/misc/agbase");
  stats.records = 0;
  DBUG_RETURN(0);
}

int ha_agbase::rnd_end()
{
  DBUG_ENTER("ha_agbase::rnd_end");
  closedir(d_dir);
  got_cond = false;
  //delete cond_tree;
  cond_vector.clear();
  DBUG_RETURN(0);
}

bool ha_agbase::cond_tree_traverser(cond_tree_node *node, GifFileType *file)
{
  DBUG_ENTER("ha_agbase::cond_tree_traverser");
  bool sibling_result;
  bool result;
  cond_tree_node *subnode;

  if (node->cmp.cmp_type == CMP_AND || node->cmp.cmp_type == CMP_OR)
  {
    subnode = node->child;
    if (node->cmp.cmp_type == CMP_AND)
    {
      sibling_result = cond_tree_traverser(node->child, file);
      for (unsigned int i = 1; i < node->child_count; i++)
      {
        sibling_result = sibling_result && cond_tree_traverser(subnode->sibling, file);
        subnode = subnode->sibling;
      }
      result = sibling_result;
    } else {
      sibling_result = cond_tree_traverser(node->child, file);
      for (unsigned int i = 1; i < node->child_count; i++)
      {
        sibling_result = sibling_result || cond_tree_traverser(subnode->sibling, file);
        subnode = subnode->sibling;
      }
      result = sibling_result;
    }
  } else {
    result = does_item_fulfil_cond(node->cmp, file);
  }

  DBUG_RETURN(result);
}

void create_condition_queue(const Item *item, void *args)
{
  DBUG_ENTER("ha_agbase::create_condition_queue");

  std::vector<const Item*> *cond_vec = (std::vector<const Item*>*)args;
  if (item != NULL)
  {
    Item::Type item_type = item->type();
    Item_func::Functype func_type;

    if (item_type == Item::COND_ITEM)
    {
      DBUG_PRINT("info", ("Found Item::COND_ITEM"));
      Item_cond *cond_item = (Item_cond*)item;
      func_type = cond_item->functype();
      if (func_type == Item_func::COND_AND_FUNC)
        DBUG_PRINT("info", ("Found COND_AND_FUNC"));
      else
        if (func_type == Item_func::COND_OR_FUNC)
          DBUG_PRINT("info", ("Found COND_OR_FUNC"));
    }

    if (item_type == Item::FUNC_ITEM)
    {
      Item_func *func_item = (Item_func*)item;
      func_type = func_item->functype();
      DBUG_PRINT("info", ("Found Item::FUNC_ITEM = [%s]", func_item->func_name()));
    }

    if (item_type == Item::INT_ITEM)
    {
      Item_int *int_item = (Item_int*)item;
      DBUG_PRINT("info", ("Found Item::INT_ITEM = %lld", int_item->val_int()));
    }

    if (item_type == Item::FIELD_ITEM)
    {
      Item_field *field_item = (Item_field*)item;
      DBUG_PRINT("info", ("Found Item::FIELD_ITEM = %s", field_item->field_name));
    }

    if (item_type == Item::NULL_ITEM)
    {
      DBUG_PRINT("info", ("Found Item::NULL_ITEM"));
    }

    if (item_type == Item::STRING_ITEM)
    {
      Item_string *str_item = (Item_string*)item;
      DBUG_PRINT("info", ("Found Item::STRING_ITEM = %s", str_item->name));
    }

    cond_vec->push_back(item);

  } else {
    DBUG_PRINT("info", ("Found NULL Item"));
    cond_vec->push_back(NULL);
  }

  DBUG_VOID_RETURN;
}

struct cond_tree_node *tree_convert(void *qptr)
{
  DBUG_ENTER("ha_agbase::tree_convert");
  struct cond_tree_node *master;
  struct cond_tree_node *curr;
  std::vector<const Item*> *cond_vec = (std::vector<const Item*>*)qptr;

  master = (struct cond_tree_node*)calloc(1, sizeof(cond_tree_node));
  curr = master;
  master->parent = master;

  for(std::vector<const Item*>::size_type i = 0; i != cond_vec->size(); i++)
  {
  begin:
    if ((*cond_vec)[i] == NULL)
      break;

    Item::Type item_type = ((*cond_vec)[i]->type());
    if (item_type == Item::COND_ITEM)
    {
      Item_func *item_func = (Item_func*)(*cond_vec)[i];
      curr->cmp.cmp_type = get_func_type(item_func->functype());
      curr->child = (cond_tree_node*)calloc(1, sizeof(cond_tree_node));
      curr->child->parent = curr;
      curr->child_count++;
      DBUG_PRINT("info", ("Cond func = %s", item_func->func_name()));
      curr = curr->child;
    } else {

      while ((*cond_vec)[i] != NULL)
      {
        Item_func *item_func;
        Item_field *item_field;
        Item_int *item_int;
        item_type = ((*cond_vec)[i])->type();

        switch(item_type) {
          case Item::FUNC_ITEM:
            item_func = (Item_func*)(*cond_vec)[i];
            curr->cmp.cmp_type = get_func_type(item_func->functype());
            break;

          case Item::FIELD_ITEM:
            item_field = (Item_field*)(*cond_vec)[i];
            curr->cmp.col_name = item_field->field_name_or_null();
            break;

          case Item::INT_ITEM:
            item_int = (Item_int*)(*cond_vec)[i];
            curr->cmp.value = item_int->val_int();

            DBUG_PRINT("info", ("func = %s | field = %s | int = %lld", item_func->func_name(), item_field->field_name, item_int->value));

            if ((*cond_vec)[i+1] != NULL && (*cond_vec)[i+1]->type() == Item::FUNC_ITEM)
            {
              curr->sibling = (cond_tree_node*)calloc(1, sizeof(cond_tree_node));
              curr->sibling->parent = curr->parent;
              curr->parent->child_count++;
              curr = curr->sibling;
            } else {
              while(curr->cmp.cmp_type != CMP_AND && curr->cmp.cmp_type != CMP_OR)
              {
                curr = curr->parent;
              }
              curr->sibling = (cond_tree_node*)calloc(1, sizeof(cond_tree_node));
              curr->sibling->parent = curr->parent;
              curr->parent->child_count++;
              curr = curr->sibling;
              i += 2;
              goto begin;
            }
            break;

          default:
            break;
        }

        i++;
      }
    }
  }

  DBUG_RETURN(master);
}

bool ha_agbase::does_cond_accept_row(GifFileType *file)
{
  DBUG_ENTER("ha_agbase::does_cond_accept_row");
  bool row_is_match = true;
  Item_bool_func2 *tmp_cond = static_cast<Item_bool_func2 *>(const_cast<COND*>(condition->cond));

  if (tmp_cond->functype() != Item_func::COND_AND_FUNC &&
      tmp_cond->functype() != Item_func::COND_OR_FUNC)
  {
    // Simple condition
    DBUG_PRINT("info", ("ha_agbase::does_cond_accept_row: Simple Condition"));
    Item **arguments;
    COND_CMP_DATA simple_cond;
    arguments = tmp_cond->arguments();
    simple_cond.col_name = ((Item_field*)arguments[0])->name;
    simple_cond.value = ((Item_int*)arguments[1])->value;
    simple_cond.cmp_type = get_func_type(tmp_cond->functype());

    row_is_match = does_item_fulfil_cond(simple_cond, file);
  } else {
    row_is_match = cond_tree_traverser(cond_tree, file);
  }

  DBUG_RETURN(row_is_match);
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.
  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.
  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_agbase::rnd_next(uchar *buf)
{
  int rc = HA_ERR_END_OF_FILE;
  struct dirent *dirent;
  my_bitmap_map *org_bitmap;
  String dirstring;

  DBUG_ENTER("ha_agbase::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  rc = 0;

  org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);

  if (d_dir != NULL)
  {
    get:
    if ((dirent = readdir(d_dir)) != NULL)
    {
      // Only operate on gif files
      while(!has_gif_extension(dirent->d_name))
      {
        if (dirent == NULL)
        {
          rc = HA_ERR_END_OF_FILE;
          goto end;
        }
        dirent = readdir(d_dir);
      }

      // Prepare the directory string
      dirstring.append("/home/ag/misc/agbase/");
      dirstring.append(dirent->d_name);
      GifFileType *file = DGifOpenFileName(dirstring.c_ptr());

      if (file != NULL)
      {
        if (got_cond)
        {
          if(!does_cond_accept_row(file))
          {
            dirstring.length(0);
            goto get;
          }
        }

        // Pack in the null bytes
        memset(buf, 0, table->s->null_bytes);

        for (Field **field = table->field; *field; field++)
        {
            buffer.length(0);
            buffer.append(dirent->d_name);

            if (!strcmp((*field)->field_name, "name"))
            {
              (*field)->store(buffer.ptr(), buffer.length(), buffer.charset());
            }
            else
              if(!strcmp((*field)->field_name, "height"))
                (*field)->store(file->SHeight);
              else
              if (!strcmp((*field)->field_name, "width"))
                (*field)->store(file->SWidth);
        }
      }
      stats.records++;
    }
    else
      rc = HA_ERR_END_OF_FILE;
  }
  else
  {
    rc = HA_ERR_END_OF_FILE;
  }

end:
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);
  MYSQL_READ_ROW_DONE(rc)
  DBUG_RETURN(rc);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode
  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.
  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.
  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_agbase::position(const uchar *record)
{
  DBUG_ENTER("ha_agbase::position");
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.
  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.
  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_agbase::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_agbase::rnd_pos");
  rc= HA_ERR_WRONG_COMMAND;
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.
  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.
  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.
  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.
  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_agbase::info(uint flag)
{
  DBUG_ENTER("ha_agbase::info");
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.
    @see
  ha_innodb.cc
*/
int ha_agbase::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_agbase::extra");
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.
  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().
  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_agbase::delete_all_rows()
{
  DBUG_ENTER("ha_agbase::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.
  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().
  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_agbase::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_agbase::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.
  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).
  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).
  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.
  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.
  Called from lock.cc by get_lock_data().
  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)
  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_agbase::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.
  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().
  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.
  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_agbase::delete_table(const char *name)
{
  DBUG_ENTER("ha_agbase::delete_table");
  /* This is not implemented but we want someone to be able that it works. */
  DBUG_RETURN(0);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.
  @details
  end_key may be empty, in which case determine if start_key matches any rows.
  Called from opt_range.cc by check_quick_keys().
  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_agbase::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_agbase::records_in_range");
  DBUG_RETURN(10);                         // low number to force index usage
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.
  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.
  Called from handle.cc by ha_create_table().
  @see
  ha_create_table() in handle.cc
*/

int ha_agbase::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
#ifndef DBUG_OFF
  ha_table_option_struct *options= table_arg->s->option_struct;
  DBUG_ENTER("ha_agbase::create");
  /*
    This example shows how to support custom engine specific table and field
    options.
  */
  DBUG_ASSERT(options);
  DBUG_PRINT("info", ("strparam: '%-.64s'  ullparam: %llu  enumparam: %u  "\
                      "boolparam: %u",
                      (options->strparam ? options->strparam : "<NULL>"),
                      options->ullparam, options->enumparam, options->boolparam));
  for (Field **field= table_arg->s->field; *field; field++)
  {
    ha_field_option_struct *field_options= (*field)->option_struct;
    DBUG_ASSERT(field_options);
    DBUG_PRINT("info", ("field: %s  complex: '%-.64s'",
                         (*field)->field_name,
                         (field_options->complex_param ?
                          field_options->complex_param :
                          "<NULL>")));
  }
#endif
  DBUG_RETURN(0);
}

const COND *ha_agbase::cond_push(const COND *cond)
{
  DBUG_ENTER("ha_agbase::cond_push");
  cond_check = false;

  if (cond)
  {
    AGBASE_CONDITION *tmp_cond;
    if (!(tmp_cond = (AGBASE_CONDITION*)malloc(sizeof(AGBASE_CONDITION*))))
      DBUG_RETURN(cond);

    tmp_cond->cond = (COND *)cond;
    condition = tmp_cond;
    got_cond = true;

    // Traverse the condition we got from the server and parse it into a linked list
    condition->cond->traverse_cond(&create_condition_queue, &cond_vector, Item::PREFIX);
    cond_vector.push_back(NULL);
    if (cond_vector.front()->type() == Item::COND_ITEM)
    {
      cond_tree = tree_convert(&cond_vector);
    }
  }
  DBUG_RETURN(NULL);
}

void ha_agbase::cond_pop()
{
  DBUG_ENTER("ha_agbase::cond_pop");
  if (condition)
  {
    AGBASE_CONDITION *tmp_cond = condition->next;
    free(condition);
    condition = tmp_cond;
  }
  DBUG_VOID_RETURN;
}

/**
  check_if_supported_inplace_alter() is used to ask the engine whether
  it can execute this ALTER TABLE statement in place or the server needs to
  create a new table and copy th data over.
  The engine may answer that the inplace alter is not supported or,
  if supported, whether the server should protect the table from concurrent
  accesses. Return values are
    HA_ALTER_INPLACE_NOT_SUPPORTED
    HA_ALTER_INPLACE_EXCLUSIVE_LOCK
    HA_ALTER_INPLACE_SHARED_LOCK
    etc
*/

enum_alter_inplace_result
ha_agbase::check_if_supported_inplace_alter(TABLE* altered_table,
                                             Alter_inplace_info* ha_alter_info)
{
  HA_CREATE_INFO *info= ha_alter_info->create_info;
  DBUG_ENTER("ha_agbase::check_if_supported_inplace_alter");

  if (ha_alter_info->handler_flags & Alter_inplace_info::CHANGE_CREATE_OPTION)
  {
    /*
      This example shows how custom engine specific table and field
      options can be accessed from this function to be compared.
    */
    ha_table_option_struct *param_new= info->option_struct;
    ha_table_option_struct *param_old= table->s->option_struct;

    /*
      check important parameters:
      for this example engine, we'll assume that changing ullparam or
      boolparam requires a table to be rebuilt, while changing strparam
      or enumparam - does not.
      For debugging purposes we'll announce this to the user
      (don't do it in production!)
    */
    if (param_new->ullparam != param_old->ullparam)
    {
      push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                          ER_UNKNOWN_ERROR, "AGBASE DEBUG: ULL %llu -> %llu",
                          param_old->ullparam, param_new->ullparam);
      DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }

    if (param_new->boolparam != param_old->boolparam)
    {
      push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                          ER_UNKNOWN_ERROR, "AGBASE DEBUG: YESNO %u -> %u",
                          param_old->boolparam, param_new->boolparam);
      DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }
  }

  if (ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_OPTION)
  {
    for (uint i= 0; i < table->s->fields; i++)
    {
      ha_field_option_struct *f_old= table->s->field[i]->option_struct;
      ha_field_option_struct *f_new= info->fields_option_struct[i];
      DBUG_ASSERT(f_old);
      if (f_new)
      {
        push_warning_printf(ha_thd(), Sql_condition::WARN_LEVEL_NOTE,
                            ER_UNKNOWN_ERROR, "AGBASE DEBUG: Field %`s COMPLEX '%s' -> '%s'",
                            table->s->field[i]->field_name,
                            f_old->complex_param,
                            f_new->complex_param);
      }
      else
        DBUG_PRINT("info", ("old field %i did not changed", i));
    }
  }

  DBUG_RETURN(HA_ALTER_INPLACE_EXCLUSIVE_LOCK);
}

bool ha_agbase::has_gif_extension(char const *name)
{
  size_t len = strlen(name);
  return len > 4 && strcmp(name + len - 4, ".gif") == 0;
}

COMPARISON_TYPE get_func_type(Item_func::Functype const type)
{
  DBUG_ENTER("ha_agbase::get_func_type");
  switch(type) {
    case Item_func::EQ_FUNC:
      DBUG_RETURN(CMP_EQ);
    case Item_func::LT_FUNC:
      DBUG_RETURN(CMP_LT);
    case Item_func::GT_FUNC:
      DBUG_RETURN(CMP_GT);
    case Item_func::LE_FUNC:
      DBUG_RETURN(CMP_LE);
    case Item_func::GE_FUNC:
      DBUG_RETURN(CMP_GE);
    case Item_func::NE_FUNC:
      DBUG_RETURN(CMP_NE);
    case Item_func::COND_AND_FUNC:
      DBUG_RETURN(CMP_AND);
    case Item_func::COND_OR_FUNC:
      DBUG_RETURN(CMP_OR);
    default:
      DBUG_RETURN(CMP_ERR);
  }
}

bool ha_agbase::does_item_fulfil_cond(COND_CMP_DATA &cmp_data, GifFileType *file)
{
  bool ret = false;
  switch(cmp_data.cmp_type)
  {
    case CMP_EQ:
      if (!strcmp(cmp_data.col_name, "width"))
        if (file->SWidth == cmp_data.value)
          ret = true;
      if (!strcmp(cmp_data.col_name, "height"))
        if (file->SHeight == cmp_data.value)
          ret = true;
      break;
    case CMP_GT:
      if (!strcmp(cmp_data.col_name, "width"))
        if (file->SWidth > cmp_data.value)
          ret = true;
      if (!strcmp(cmp_data.col_name, "height"))
        if (file->SHeight > cmp_data.value)
          ret = true;
      break;
    case CMP_LT:
      if (!strcmp(cmp_data.col_name, "width"))
        if (file->SWidth < cmp_data.value)
          ret = true;
      if (!strcmp(cmp_data.col_name, "height"))
        if (file->SHeight < cmp_data.value)
          ret = true;
      break;
    case CMP_GE:
      if (!strcmp(cmp_data.col_name, "width"))
        if (file->SWidth >= cmp_data.value)
          ret = true;
      if (!strcmp(cmp_data.col_name, "height"))
        if (file->SHeight >= cmp_data.value)
          ret = true;
      break;
    case CMP_LE:
      if (!strcmp(cmp_data.col_name, "width"))
        if (file->SWidth <= cmp_data.value)
          ret = true;
      if (!strcmp(cmp_data.col_name, "height"))
        if (file->SHeight <= cmp_data.value)
          ret = true;
      break;
    case CMP_NE:
      if (!strcmp(cmp_data.col_name, "width"))
        if (file->SWidth != cmp_data.value)
          ret = true;
      if (!strcmp(cmp_data.col_name, "height"))
        if (file->SHeight != cmp_data.value)
          ret = true;
      break;
    default:
      break;
  }
  return ret;
}

struct st_mysql_storage_engine agbase_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_THDVAR_INT(int_var, PLUGIN_VAR_RQCMDARG, "-1..1",
  NULL, NULL, 0, -1, 1, 0);

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  double_var,
  srv_double_var,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);                             // reserved always 0

static MYSQL_THDVAR_DOUBLE(
  double_thdvar,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);

static struct st_mysql_sys_var* agbase_system_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  MYSQL_SYSVAR(int_var),
  MYSQL_SYSVAR(double_var),
  MYSQL_SYSVAR(double_thdvar),
  MYSQL_SYSVAR(varopt_default),
  NULL
};

// this is an example of SHOW_SIMPLE_FUNC and of my_snprintf() service
// If this function would return an array, one should use SHOW_FUNC
static int show_func_example(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, int_var is %d, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, THDVAR(thd, int_var),
              srv_double_var, "really");
  return 0;
}

static struct st_mysql_show_var func_status[]=
{
  {"func_example",  (char *)show_func_example, SHOW_SIMPLE_FUNC},
  {0,0,SHOW_UNDEF}
};

struct st_mysql_daemon unusable_example=
{ MYSQL_DAEMON_INTERFACE_VERSION };

// DECLARE MYSQL PLUGIN
mysql_declare_plugin(agbase)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&agbase_storage_engine,
	"AGBASE",
	"Alessandro Gangemi",
	"AG Storage Engine",
	PLUGIN_LICENSE_GPL,
	agbase_init_func,
	NULL,
	0x0001,
	func_status,
	agbase_system_variables,
	NULL,
	0,
}
mysql_declare_plugin_end;
maria_declare_plugin(agbase)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&agbase_storage_engine,
	"AGBASE",
	"Alessandro Gangemi",
	"Agbase Storage Engine",
	PLUGIN_LICENSE_GPL,
	agbase_init_func,
	NULL,
	0x0001,
	func_status,
	agbase_system_variables,
	"0.1",
	MariaDB_PLUGIN_MATURITY_EXPERIMENTAL
}
maria_declare_plugin_end;
