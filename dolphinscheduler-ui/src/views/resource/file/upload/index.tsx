/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { defineComponent, toRefs, PropType, getCurrentInstance,watch } from 'vue'
import { NButton, NForm, NFormItem, NInput, NUpload } from 'naive-ui'

import { useI18n } from 'vue-i18n'
import Modal from '@/components/modal'
import { useForm } from './use-form'
import { useUpload } from './use-upload'
import { fi } from 'date-fns/locale'

const props = {
  show: {
    type: Boolean as PropType<boolean>,
    default: false
  },
  id: {
    type: Number as PropType<number>,
    default: -1
  },
  name: {
    type: String as PropType<string>,
    default: ''
  },
  description: {
    type: String as PropType<string>,
    default: ''
  }
}

export default defineComponent({
  name: 'ResourceFileUpload',
  props,
  emits: ['updateList', 'update:show'],
  setup(props, ctx) {
    const { state, resetForm } = useForm(props.id,props.name,props.description)
    const { handleUploadFile } = useUpload(state)

    const hideModal = () => {
      resetForm()
      state.newFile =[]
      ctx.emit('update:show')
    }
    const getSuffex = (name:string)=>{
      const i = name.lastIndexOf('.')
      const a = name.substring(i, name.length)
      return a;
    }
    const customRequest = ({ file }: any) => {
        if(state.uploadForm.id != -1 && getSuffex(state.uploadForm.name)!= getSuffex(file.name)){
          window.$message.success('文件类型前后不一致')
          state.uploadForm.file=''
          state.newFile = []
          return;
        }else{
           state.uploadForm.name = file.name
           state.uploadForm.file = file.file
           state.uploadFormRef.validate()
        }
        
    }

    const handleFile = () => {
      handleUploadFile(ctx.emit, hideModal, resetForm)
    }


    const removeFile = () => {
      if(state.uploadForm.id == -1){
        state.uploadForm.name = ''
      }
      state.uploadForm.file = ''
    }

    const trim = getCurrentInstance()?.appContext.config.globalProperties.trim

    watch(
      () => props.show,
      () => {
        state.uploadForm.id = props.id
        state.uploadForm.name = props.name
        state.uploadForm.file ='file'
        state.uploadForm.description = props.description
      }
    )

    return {
      hideModal,
      customRequest,
      handleFile,
      removeFile,
      ...toRefs(state),
      trim
    }
  },
  render() {
    const { t } = useI18n()
    return (
      <Modal
        show={this.$props.show}
        title={t('resource.file.upload_files')}
        onCancel={this.hideModal}
        onConfirm={this.handleFile}
        confirmClassName='btn-submit'
        cancelClassName='btn-cancel'
        confirmLoading={this.saving}
      >
        <NForm rules={this.rules} ref='uploadFormRef'>
          <NFormItem
            label={t('resource.file.file_name')}
            path='name'
            ref='uploadFormNameRef'
          >
            <NInput
              allowInput={this.trim}
              v-model={[this.uploadForm.name, 'value']}
              placeholder={t('resource.file.enter_name_tips')}
              class='input-file-name'
            />
          </NFormItem>
          <NFormItem label={t('resource.file.description')} path='description'>
            <NInput
              allowInput={this.trim}
              type='textarea'
              v-model={[this.uploadForm.description, 'value']}
              placeholder={t('resource.file.enter_description_tips')}
              class='input-description'
            />
          </NFormItem>
          <NFormItem label={t('resource.file.upload_files')} path='file'>
            <NUpload
              v-model:file-list={this.newFile}
              customRequest={this.customRequest}
              class='btn-upload'
              max={1}
              onRemove={this.removeFile}
            >
              <NButton>{t('resource.file.upload_files')}</NButton>
            </NUpload>
          </NFormItem>
        </NForm>
      </Modal>
    )
  }
})
